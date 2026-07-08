# Exactly-Once in Kafka: transactions in plain words

## 1. Where duplicates come from in the first place

Picture a bank transfer. The producer tells the broker: "write the
message 'debit 100€'". The broker writes it, but the **acknowledgement
(ack) is lost** on the way back:

```
Producer                          Broker
   |                                |
   |  send("debit 100€")   ------>  |  [written ✓]
   |                                |
   |  <---- ack (LOST ✗) ---------  |
   |                                |
   |  timeout! retry...             |
   |  send("debit 100€")   ------>  |  [written AGAIN ✓✗]
   |                                |
   |  <-------- ack --------------- |
```

The producer doesn't know whether the first message arrived. It
retries — and the log ends up with **two records**. The customer is
debited 200€ instead of 100€.

Hence three levels of delivery guarantee:

| Guarantee | Meaning | Risk |
|---|---|---|
| **at-most-once** | at most one time | can lose |
| **at-least-once** | at least one time | can duplicate |
| **exactly-once** | exactly one time | nothing |

By default Kafka gives at-least-once. Exactly-once (EOS) is something we
turn on deliberately.

---

## 2. Two different sources of duplicates

Understand this up front: duplicates are born in **two places**, and
they are cured differently.

```
                SOURCE 1                          SOURCE 2
           producer-side dups                consumer-side dups

   Producer --retry--> Broker        Broker --> Consumer --process--> Broker
      (retry on lost ack)                  (processed, but crashed BEFORE
                                            committing the offset → re-read)
```

- **Source 1 (producer-side)** — retry on a lost ack. Cured by the
  **idempotent producer**.
- **Source 2 (consumer-side)** — the read-process-write pattern: read →
  process → write the result → and only then commit the offset. If you
  crash between "wrote" and "committed", after a restart you re-read the
  same message and process it twice. Cured by **transactions**.

Remember this split — half the confusion around EOS comes from mixing
these two problems together.

---

## 3. The idempotent producer (cures source 1)

Idempotence is turned on with one setting (since Kafka 3.0 it is **on by
default**):

```java
props.put("enable.idempotence", "true");
```

How it works: the broker gives the producer a **PID** (producer id) and
numbers every message with a **sequence number**. The broker remembers
the last number per partition:

```
Producer (PID=42)                 Broker (partition 0)
   |                                |  last seq: 4
   |  msg seq=5  ------------------>|  5 > 4 → written ✓, remember seq=5
   |  <---- ack (lost ✗) -----------|
   |                                |
   |  retry: msg seq=5  ----------->|  5 == 5 → ALREADY SEEN, drop duplicate
   |  <-------- ack ----------------|      (no write to the log)
```

A repeated message with the same `seq` is silently dropped by the
broker. No duplicate ends up in the log.

**The boundary:** idempotence saves you from duplicates *within one
producer session, at the retry level*. It does **not** save you if your
application itself called `send()` twice because of its own logic. It is
not "magic against all duplicates".

---

## 4. Transactions (cure source 2)

A transaction lets you write to **several partitions/topics atomically**
— all or nothing. Plus atomically commit the offsets of the messages you
read.

```java
producer.initTransactions();          // once at startup
producer.beginTransaction();
producer.send(record1);               // to topic A
producer.send(record2);               // to topic B
producer.sendOffsetsToTransaction(    // + the offset of what was read
    offsets, consumerGroupMetadata);
producer.commitTransaction();         // everything becomes visible at once
```

Read-process-write atomically:

```
        read               process               write + commit offset
   ┌───────────┐        ┌─────────────┐        ┌──────────────────────┐
   │ input     │ ─────> │ your logic  │ ─────> │ output + offset       │
   │ topic     │        └─────────────┘        │ IN ONE transaction    │
   └───────────┘                               └──────────────────────┘
                                                  commit / abort
```

If the application crashes mid-way — the transaction is **not
committed**, the broker **aborts** it, the offset does not move. After a
restart you re-read the input from the same place and process it again,
but nobody ever saw the old (uncommitted) result. No duplicate on the
output.

The key point: `send()` of the result and "commit of the input offset"
are **in one transaction**. That is why the consumer's
`enable.auto.commit` must be **false** — offsets are managed by the
transaction, not by the auto-committer.

---

## 5. The zombie problem and epoch-based fencing

The worst case: the application "froze" (a GC pause, the network), the
orchestrator decided it was dead, and started a **second copy**. Now two
producers with the same `transactional.id` write at the same time — the
"zombie" and the "live" one.

Kafka solves this with **epochs**. On every new `initTransactions()` for
a given `transactional.id`, the broker raises the epoch and "fences off"
the old one:

```
Producer-A (epoch 5)  ──write──>  Broker
                                    |  current epoch for txn.id = 5

[copy B started]
Producer-B (initTransactions)  ─>  Broker: new epoch = 6
                                    |  current epoch = 6

Producer-A (zombie, epoch 5) ─write─> Broker: 5 < 6 →
                                    ProducerFencedException ✗
                                    (zombie dies, cannot write)
```

The result in the log is `A B`, not `A B B`. The zombie physically
cannot commit.

---

## 6. How the consumer reads this: read_committed and the LSO

The producer writes — but until the transaction is committed, the reader
**must not** see those messages. The setting on the consumer side:

```java
props.put("isolation.level", "read_committed");
```

The broker keeps a pointer called the **LSO (Last Stable Offset)** — the
offset of the first record of the earliest still-open transaction. A
consumer with `read_committed` reads **only up to the LSO**:

```
partition (offset →):   0    1    2    3    4    5
                        A    B    T?   T?   C    ...
                                  ↑
                        txn open (2,3)     ← LSO parks here
                                             consumer sees only 0,1
                        until the txn commits, 2..3 are invisible,
                        and everything AFTER them waits too
```

Subtlety (a common mistake in slides): even a **non-transactional**
message that landed *after* an open transaction started is not delivered
to the reader until that transaction finishes — the LSO does not move.
It is not "a message outside the transaction is visible immediately".

`read_uncommitted` (the default) sees everything as it comes, including
what later gets aborted.

---

## 7. Where EOS ends

The main limitation you must state honestly to students:

```
   Kafka  ──EOS works──>  Kafka        ✓  (transactions cover it)

   Kafka  ──> your code ──> external DB / HTTP / email   ✗
              (Kafka will NOT roll back an external side-effect)
```

Exactly-once in Kafka is a **Kafka → Kafka** guarantee. The moment you
write to an external system (a database, a payment API, sending an
email), Kafka cannot roll it back. There EOS is achieved by
**idempotence on the receiver side** — for example, a
`processed_message(id)` table with a check of "have I already processed
this message".

---

**In short, what to remember:**

- duplicates from two places: producer-retry and read-process-write
- `enable.idempotence=true` (default in 3.0+) → cures retry-dups via PID+seq
- transactions → atomic write + offset commit, `auto.commit=false`
- epochs → protection against zombies (`ProducerFencedException`)
- `read_committed` + LSO → the reader sees only finished transactions
- EOS = Kafka→Kafka only; external systems need their own idempotence
