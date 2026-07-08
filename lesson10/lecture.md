# Lesson 10 — Transactions and Exactly-Once

The previous lesson ended on an open question. Manual commit after
processing gives **at-least-once**: a crash between processing and
committing reprocesses the batch, never loses it — but the duplicate
is real, and the lecture closed with "exactly-once needs
transactions". This is that lesson.

Exactly-once in Kafka is not one feature. It is two independent
mechanisms that solve two different duplication problems, plus a
fencing trick that solves a third:

- **Idempotent producer** — kills duplicates created by the client
  library retrying a send after a lost `ack`. Per-partition,
  on by default since 3.0, essentially free.
- **Transactions** — make an atomic unit out of "write to N topics
  **and** commit the input offset". This is what closes the
  read-process-write loop so a message is processed exactly once.
- **Fencing** — stops a stalled-then-resurrected instance (a
  "zombie") from writing duplicates after the group has moved on.

The companion lab (`LAB.md`) runs eight small Java programs (`Ex1`–
`Ex8`) against the existing `kafka` EC2 broker (`localhost:9092`,
SASL/PLAIN). Unlike lessons 8–9, transactions need **new
infrastructure**: transactional-id ACLs and dedicated inbound/outbound
topics, added through Terraform (`gitops/`) — the transactional
producer also needs the broker's `__transaction_state` topic to exist,
which on a single-node broker requires two non-default settings (§10).

## Learning objectives

After this lesson you will be able to:

- State the three delivery guarantees precisely and name where each
  duplicate actually comes from — the producer side and the consumer
  side are different problems.
- Explain how the idempotent producer deduplicates with `PID` +
  sequence number, what it guarantees, and the exact boundary where it
  stops helping.
- Use transactions to make read-process-write atomic: the API calls,
  their order, and why `sendOffsetsToTransaction` is the piece that
  makes it exactly-once.
- Describe how a transaction executes internally — the transaction
  coordinator, the `__transaction_state` log, and the commit markers.
- Explain the zombie problem and how epoch-based fencing
  (`transactional.id`) prevents it.
- Set `isolation.level` correctly on the consumer and explain what the
  Last Stable Offset does.
- State the one hard limit of Kafka exactly-once — it is Kafka→Kafka
  only — and the standard workaround when an external system is in the
  loop.

---

## 1. Three guarantees, two sources of duplicates

The three delivery semantics, in order of strength:

- **at-most-once** — a message may be lost, never duplicated. Fire and
  forget; don't retry.
- **at-least-once** — a message is never lost, but may be delivered
  more than once. Retry until acknowledged. This is the common
  default and what lesson 9's manual commit gave you.
- **exactly-once** — delivered and processed once, no loss, no
  duplicate. The hard one — a running joke in distributed systems for
  a reason.

In a Kafka pipeline duplicates enter from **two independent places**,
and each needs a different fix:

```
   ┌────────────┐        write         ┌──────────────┐   read at offset   ┌────────────┐
   │  Producer  │ ───────────────────► │  partition   │ ─────────────────► │  Consumer  │
   │            │  (1) may write the   │  [.....]     │  (2) may read the  │            │
   └────────────┘      same record     └──────────────┘      same record   └────────────┘
                       twice on retry                        twice on restart
```

1. **Producer duplicates** — the producer sent a record, the broker
   stored it and replied `ack`, the `ack` was lost on the network, and
   the client library retried the same record. Fixed by the
   **idempotent producer** (§2).

2. **Consumer duplicates** — the consumer processed a record, then
   crashed before committing its offset; on restart it reads the same
   record again. Not a producer problem at all; fixed for the
   read-process-write case by **transactions** (§4).

Keep these separate. Idempotence does nothing for problem 2, and
transactions are overkill for problem 1.

---

## 2. The idempotent producer

Walk the failure precisely. Idempotence off:

```
   Producer                         Broker / partition
      │  send(bar) ─────────────────►│  append bar   log:[bar]
      │                              │
      │  ◄───────── ack ✗ (lost) ────│   (broker did its job; ack never arrives)
      │                              │
      │  retry(bar) ────────────────►│  append bar   log:[bar, bar]   ← DUPLICATE
```

The broker cannot tell the retry apart from a genuinely new record —
there is nothing on the record that says "you already have this".

With `enable.idempotence=true`, two identifiers are added:

- **PID** — a producer id the broker assigns at init, unique to this
  producer session.
- **Seq** — a sequence number per partition, incrementing per record.

The broker remembers the highest `Seq` it has accepted for each
`(PID, partition)` and rejects anything it has already seen:

```
   Producer                              Broker / partition
      │  send(bar) PID:5 Seq:1 ─────────►│  append   log:[bar@Seq1]
      │  ◄────────── ack ✗ (lost) ───────│
      │  retry(bar) PID:5 Seq:1 ────────►│  Seq:1 already seen → DROP, re-ack
      │  ◄────────── ack ────────────────│  log:[bar@Seq1]   ← still one copy
```

Result is **exactly-once *per partition, per producer session***, and
ordering is preserved. Enabled by default since **3.0** (KIP-679). The
overhead — a few bytes of PID/Seq per batch — is negligible.

### The constraints

Idempotence requires a consistent configuration, or `KafkaProducer`
refuses to construct:

- `acks=all` — without the full-ISR ack, dedup has nothing solid to
  anchor to.
- `max.in.flight.requests.per.connection <= 5`.
- `retries > 0`.

Since 3.0 these are set automatically when idempotence is on (and it
is on by default). `Ex3InvalidConfig` sets `max.in.flight=6` with
idempotence on and shows the `ConfigException` at construction time.

> **Correction to a common slide.** The `max.in.flight <= 5` ceiling
> is sometimes said to arrive "in 2.0+". It is **1.0.0** — before
> that, idempotence forced `max.in.flight=1`; KAFKA-5494 raised it to
> 5 while still preserving order via the sequence numbers. On the 4.x
> broker in the lab, "≤ 5" is simply the rule; the version footnote is
> archaeology.

> **Correction to a common slide.** `acks=all` alone is frequently
> presented as "durable". On the single-broker lab (RF=1) `acks=all`
> waits for exactly one replica — it is a durability *illusion* there.
> Real durability needs RF ≥ 3 **and** `min.insync.replicas=2`. The
> idempotence/EOS guarantees in this lab are pedagogically correct but
> not production-durable, because the cluster is one node.

### Where idempotence stops

The idempotent producer only dedups **the library's own retries**. If
`send()` ultimately fails and your application catches the error and
resends the record itself, that is a **new** record with a new `Seq` —
the broker stores it. Idempotence gives you nothing against
duplicates created by application-level retry logic.

```
   send(bar) ──► library retries internally ──► deduped by PID/Seq   ✅
   send(bar) ──► fails ──► your code catches, resends ──► new record  ❌ duplicate
```

`Ex4ProducerError` demonstrates this: a fresh producer restarts and
replays from an earlier key, and the duplicates land — idempotence
does not span producer sessions or application retries. This gap is
the motivation for transactions.

---

## 3. The read-process-write problem

The pattern transactions exist for:

```
   inbound topic        Application 1 (may run in several instances)     outbound topic
   ┌───────────┐        ┌───────────────────────────────────────┐       ┌────────────┐
   │           │───────►│  consume ──► business logic ──► produce│──────►│            │
   └───────────┘        └───────────────────────────────────────┘       └────────────┘
```

The app reads a message, transforms it, writes the result to another
topic, and records progress by committing the input offset.
Application 1 may be stateless (no database), and may run in several
instances in one consumer group. The requirement: **each inbound
message is processed, and its outbound message written, exactly once,
in the order the inbound messages were read**.

Naively — produce, then commit the offset — has a hole. If the app
writes to outbound and then crashes before committing the offset, on
restart it reprocesses the same inbound message and writes the
outbound record **again**. Idempotence does not help: the second write
is a new record from a new producer session.

And there is a nastier failure, the **zombie**:

| Instance 1 | Instance 2 | Kafka | Outbound |
|---|---|---|---|
| gets messages 1, 2 | | | |
| processes 1, sends A, commits offset | | | A |
| enters a long GC pause | | loses instance 1, rebalances | |
| | gets message 2 | | |
| wakes up, processes 2, sends B | processes 2, sends B | | A **B B** |

Instance 1 stalled long enough (a stop-the-world GC) that the
coordinator declared it dead and moved its partition to instance 2.
Both then process message 2 and both write B → outbound has a
duplicate. The idempotent producer cannot catch this: the two B's come
from two different producers with different PIDs, so the broker sees
two legitimate writes.

Two things are needed: make "write outbound + commit input offset" a
single atomic step (§4), and stop the zombie from writing after it has
been replaced (§6).

---

## 4. Transactions

A transaction lets a producer write to one or more topics such that
**all of the writes commit, or none do**. The trick that makes it
solve read-process-write: **committed offsets are themselves records**
(in `__consumer_offsets`), so committing the input offset can be made
part of the same transaction as the output writes.

```
   ┌─────────────────── one atomic transaction ───────────────────┐
   │  produce → outbound        (the result)                       │
   │  commit input offset       (progress on inbound)              │
   └───────────────────────────────────────────────────────────────┘
              both land, or neither does
```

### The API

```java
producer.initTransactions();                    // once, before anything
...
producer.beginTransaction();
producer.send(new ProducerRecord<>("outbound", key, value));
producer.sendOffsetsToTransaction(offsets, groupMetadata);  // input progress
producer.commitTransaction();                   // or abortTransaction()
```

- `ProducerConfig.TRANSACTIONAL_ID_CONFIG` must be set — it turns on
  transactions and **implies idempotence** (a transactional producer
  is always idempotent).
- `initTransactions()` — called once before use; registers the
  `transactional.id` with the coordinator and fences any previous
  producer using the same id.
- `beginTransaction()` / `send()` — sends inside the open transaction.
- `sendOffsetsToTransaction(offsets, groupMetadata)` — commits the
  consumer's input offsets **as part of the transaction**. This is the
  piece that makes the loop exactly-once.
- `commitTransaction()` / `abortTransaction()` — blocks until all
  buffered sends are flushed, then finalizes or discards the whole
  transaction.

For the read-process-write consumer, `enable.auto.commit` **must be
`false`** — the offset is committed through the transaction, not by the
auto-committer. `Ex5Transaction` shows a plain multi-topic atomic
write; `Ex6ReadWrite` is the full read-process-write transformer.

> **Correction to a common slide.** `sendOffsetsToTransaction` is
> often shown with a `String groupId` argument. That overload is
> deprecated. The current signature (KIP-447) takes a
> `ConsumerGroupMetadata`, which carries the consumer's group,
> generation, and member id so the offset commit participates in
> group fencing during a rebalance. `Ex6` uses the
> `ConsumerGroupMetadata` form; the `String` form should not be used
> on 4.x.

---

## 5. How a transaction executes

The producer does not write to partitions "on its own". A **transaction
coordinator** (a broker role) drives it, and the transaction's state
lives in an internal topic, `__transaction_state`. Four phases:

```
                    A                          Transaction
   ┌──────────┐  register t.id   ┌──────────────────────┐
   │ Producer │ ───────────────► │ Transaction          │
   │          │                  │ Coordinator          │
   │          │ ◄─── fence ──────│                       │
   └────┬─────┘                  └──────────┬────────────┘
        │ C  send(data)                     │ B  write state
        ▼                                    ▼
   ┌──────────┐                       ┌──────────────────┐
   │ Data logs│ ◄──── D  markers ─────│ __transaction_   │
   │ (topics) │    (commit/abort)     │ state (TL)       │
   └──────────┘                       └──────────────────┘
        m0 m1 [C]                        init → ongoing →
                                         prepare → committed
```

- **A. Producer → Coordinator.** The producer registers its
  `transactional.id`. The coordinator **aborts** any in-flight
  transaction under that id and **fences** the id (§6). The first time
  the producer writes to a new partition, it tells the coordinator so
  the coordinator knows which partitions the transaction touches.
  `abort`/`commit` requests also go to the coordinator.
- **B. Coordinator → Transaction Log.** All of that state is written to
  `__transaction_state` (the TL, a compacted internal topic):
  `ongoing`, then on commit `prepare` → `committed`.
- **C. Producer → topics.** Data is written to the actual partitions,
  as uncommitted records.
- **D. Coordinator → topics.** On `commit`, the coordinator writes a
  **commit marker** into every partition the transaction touched, and
  flips the TL state to `committed`. The marker is what a
  `read_committed` consumer waits for. On `abort`, it writes abort
  markers instead and the records are skipped.

The commit is effectively two-phase: the transaction is durable once
the TL says `committed`, then the markers make it visible. If the
coordinator crashes mid-commit, it recovers the state from the TL and
finishes.

---

## 6. Fencing: defeating the zombie

The zombie from §3 is stopped by an **epoch** bound to the
`transactional.id`. Every `initTransactions()` for a given id bumps the
epoch at the coordinator; only the highest epoch is allowed to write.
Replay the same timeline, now with `transactional.id = X`:

| Instance 1 | Instance 2 | Kafka | Outbound |
|---|---|---|---|
| initTransactions (t.id = X) | | epoch = 1 | |
| gets messages 1, 2 | | | |
| processes 1, sends A | | | A |
| enters a long GC pause | | loses instance 1, rebalances | |
| | initTransactions (t.id = X) | epoch bumped to 2 | |
| | gets message 2 | | |
| wakes, processes 2, sends B | | **rejected — stale epoch** | A |
| | processes 2, sends B | | A B |

When instance 2 initialized with the same `transactional.id`, the
coordinator raised the epoch. The zombie (instance 1) still holds the
old epoch, so its `beginTransaction`/`commit` is rejected with
**`ProducerFencedException`**; its transaction aborts and B never
becomes visible. Only one producer per `transactional.id` is live at
any moment. `Ex8Fenced` reproduces this: two producers share
`transactional.id="ex8"`, the second commits, and the first then
throws `ProducerFencedException` on its next transaction.

This is the direct analogue of the zombie-consumer problem, solved the
same way — a monotonic epoch that fences the stale actor.

---

## 7. `isolation.level` and the Last Stable Offset

Transactions on the producer are half the story. The consumer must opt
in to not seeing uncommitted data:

| `isolation.level` | Behaviour |
|---|---|
| `read_uncommitted` | **default** — sees everything, including records from open and aborted transactions |
| `read_committed` | sees only committed records; skips aborted ones, waits for open ones |

> The default being `read_uncommitted` surprises people: transactions
> on the write side do nothing for a consumer that hasn't set
> `read_committed`. It is off unless you ask for it.

The mechanism is the **Last Stable Offset (LSO)**. A `read_committed`
consumer only reads a partition up to the LSO — the offset of the
first record of the earliest still-open transaction. Until that
transaction commits or aborts, the LSO does not advance, and nothing
past it is delivered:

```
   partition:  [ 0 ][ 1 ][ 2 ][ 3 ][ 4 ] ...
                              ▲
                              │ open transaction starts here
                              └── LSO parks here; read_committed reads 0..2 only,
                                  waits for commit/abort before 3, 4, ...
```

The consequence people miss: a **non-transactional** record written
*after* an open transaction still sits behind the LSO and is withheld
until that transaction resolves — even though it isn't part of any
transaction.

> **Correction to a common slide.** The commonly-given explanation
> that "a message sent outside the transaction is visible immediately"
> is only true if it landed *before* the transaction opened. Written
> after an open transaction, it is held behind the LSO until the
> transaction commits or aborts. `Ex7IsolationLevel` makes this
> visible by running a `read_committed` and a `read_uncommitted`
> consumer side by side against interleaved transactional and plain
> sends.

---

## 8. The one hard limit of exactly-once

Kafka's exactly-once machinery closes over **"read from Kafka →
transform → write to Kafka"**. The moment a non-Kafka system enters
the loop — a database, an HTTP call, a payment gateway — atomicity
breaks, because the external side is not part of the Kafka
transaction.

When that happens you fall back to **at-least-once and deduplicate
yourself**, on the external side. The standard idiom is an idempotency
table inside *your* database transaction:

```
   consume(msg id=xyz)
   ┌──────────── your DB transaction ─────────────┐
   │  INSERT INTO processed_message(msg_id='xyz')  │  ← unique key;
   │  UPDATE application_state ...                 │    duplicate INSERT fails,
   └───────────────────────────────────────────────┘    so the effect applies once
```

A replayed message tries to insert an already-present `msg_id`, the
unique constraint rejects it, and the transaction rolls back — the
side effect happens once regardless of redelivery. Exactly-once with
an external system is an application-level property, not a Kafka
feature.

---

## 9. Overhead

The cost of a transaction is fixed, not proportional to its size:

- The extra round-trips are producer→coordinator, coordinator→TL, and
  coordinator→partitions (the markers) — a constant per transaction,
  independent of how many records it contains.
- Therefore the **more** records per transaction, the **lower** the
  relative overhead. Very small transactions pay the most.
- But large transactions hold the LSO and **block `read_committed`
  consumers** on the affected partitions for longer. There is a
  latency/throughput trade-off in batch size.
- `transaction.timeout.ms` (default **60 s**) is the point at which the
  coordinator force-aborts a transaction that hasn't committed — a
  safety net against a producer that opened a transaction and vanished.

The `AdminClient` exposes `listTransactions`, `describeTransactions`,
and `abortTransaction` for operating on stuck transactions.

---

## 10. Configuration knobs

Reference. Sections cited by number.

| Setting | Side | Default | What it does |
|---|---|---|---|
| `enable.idempotence` | producer | `true` (3.0+) | PID+Seq dedup of library retries. §2 |
| `acks` | producer | `all` (3.0+) | required `all` for idempotence. §2 |
| `max.in.flight.requests.per.connection` | producer | 5 | must be ≤ 5 with idempotence. §2 |
| `transactional.id` | producer | null | enables transactions; implies idempotence; the fencing key. §4, §6 |
| `transaction.timeout.ms` | producer | 60000 | coordinator force-aborts after this. §9 |
| `isolation.level` | consumer | `read_uncommitted` | `read_committed` hides uncommitted/aborted records. §7 |
| `enable.auto.commit` | consumer | true | **must be false** for transactional read-process-write. §4 |

### Broker settings that matter on a single node

The transactional producer needs the `__transaction_state` topic to
exist. Its replication defaults assume a multi-broker cluster and will
**hang `initTransactions()`** on one node until fixed:

| Broker setting | Cluster default | Single-node lab |
|---|---|---|
| `transaction.state.log.replication.factor` | 3 | **1** |
| `transaction.state.log.min.isr` | 2 | **1** |
| `offsets.topic.replication.factor` | 3 | 1 (already set in lesson 7) |

These are the transaction-side twins of the offsets-topic setting from
lesson 7, and they are separate — setting one does not set the other.

---

## 11. Writing transactional code

The full read-process-write transformer — consume, transform, produce,
and commit the input offset, all in one transaction:

```java
producer.initTransactions();                     // once at startup

while (running) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
    if (records.isEmpty()) continue;

    producer.beginTransaction();
    try {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            producer.send(new ProducerRecord<>("outbound",
                    record.key(), transform(record.value())));
            offsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1));   // next offset to read
        }
        producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
        producer.commitTransaction();
    } catch (ProducerFencedException e) {
        producer.close();      // a newer instance fenced us — this one is done
        break;
    } catch (KafkaException e) {
        producer.abortTransaction();   // roll back, the batch will be re-read
    }
}
```

Line by line:

- **`initTransactions()` once**, before the loop — registers the id and
  fences older instances.
- **`beginTransaction()` per batch.** Everything until `commit` is one
  atomic unit.
- **`send()` the transformed records** to outbound; they are invisible
  to `read_committed` consumers until the commit marker lands.
- **`OffsetAndMetadata(offset + 1)`** — commit the *next* offset to
  read, not the last one processed. The off-by-one from lesson 5.
- **`sendOffsetsToTransaction(offsets, consumer.groupMetadata())`** —
  the input progress rides the same transaction as the output. This is
  the exactly-once join.
- **`ProducerFencedException` → close and stop.** A newer instance took
  over; this producer must not continue. Do not retry.
- **`abortTransaction()` on other failures** — roll everything back;
  the offsets were never committed, so the batch is re-read and
  retried cleanly.

The consumer here is set with `enable.auto.commit=false` and
`isolation.level=read_committed` so it neither commits behind the
transaction's back nor reads other producers' uncommitted output.

---

## 12. Summary

**Two sources of duplicates, two fixes.** Producer-side duplicates
(library retry after a lost `ack`) are killed by the **idempotent
producer** — `PID` + per-partition `Seq`, on by default since 3.0,
exactly-once *per partition per session*. Consumer-side duplicates in
read-process-write are killed by **transactions**.

**Idempotence has a hard edge.** It dedups only the library's own
retries. Application-level resends and new producer sessions are new
records — it cannot help there. That gap is why transactions exist.

**Transactions make read-process-write atomic.** Because committed
offsets are records, `sendOffsetsToTransaction` folds "write output"
and "commit input offset" into one all-or-nothing unit. Set
`transactional.id` (implies idempotence) on the producer and
`enable.auto.commit=false` on the consumer.

**Internally** a transaction coordinator drives it, state lives in
`__transaction_state`, and commit/abort **markers** in each touched
partition make the outcome visible. **Fencing** by monotonic epoch on
the `transactional.id` stops zombies: a resurrected old instance gets
`ProducerFencedException`.

**Consumers must opt in.** `isolation.level=read_committed` (default is
`read_uncommitted`) reads only up to the **Last Stable Offset** — even
a non-transactional record written after an open transaction waits
behind the LSO.

**One hard limit.** Exactly-once is Kafka→Kafka only. With an external
system in the loop, use at-least-once plus an idempotency key in your
own database transaction.

**In one line:** idempotent producer + transactions =
exactly-once for read → transform (no external DB) → write.

---

## References

### Official documentation

| Source | URL |
|---|---|
| KIP-98 — Exactly Once Delivery and Transactional Messaging | https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging |
| `KafkaProducer` transactions JavaDoc | https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html |
| Producer configs | https://kafka.apache.org/documentation/#producerconfigs |
| Confluent — how Kafka does exactly-once | https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/ |

### KIPs that shaped this

| KIP | Change | Kafka version |
|---|---|---|
| KAFKA-5494 | idempotence allows `max.in.flight` up to 5 (was 1) | 1.0.0 |
| KIP-98 | idempotent producer + transactions | 0.11 |
| KIP-447 | `sendOffsetsToTransaction(ConsumerGroupMetadata)`; fewer producers for EOS | 2.5 |
| KIP-679 | `enable.idempotence=true` by default | 3.0 |

### Book

| Source | URL |
|---|---|
| Kafka: The Definitive Guide, 2nd ed. (Shapira, Palino, Sivaram, Petty), Chapter 8 — Exactly-Once Semantics | https://www.confluent.io/resources/kafka-the-definitive-guide-v2/ |

### Versions used in the lab

- Broker on `kafka` EC2: Apache Kafka 4.3.0 (native install, KRaft single-node); `__transaction_state` RF and min.ISR set to 1 (§10)
- Client library in `transactions-java/build.gradle`: `kafka-clients` 4.0.0
- New Terraform infrastructure: inbound/outbound topics and `transactional.id` ACLs for `alice` (WRITE+DESCRIBE on TransactionalId, prefixed) plus READ on the consumer group for read-process-write
