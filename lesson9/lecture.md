# Lesson 9 — Kafka Consumer API

The previous lesson was about the **write side**: the `KafkaProducer`,
how `send()` enqueues records, how the partitioner chooses a
partition, and how `acks` and idempotence give durability.

This lesson is the **read side**. A producer's job ends when a record
is committed to a partition. A consumer's job is to read those records
back — in order, without missing any, without reading the same one
twice after a restart, and fast enough to keep up. Almost everything
that makes Kafka consumers harder than "read from a queue" comes from
one idea: **consumers work in groups, and the unit they divide is the
partition**.

The companion lab (`LAB.md`) runs five small Java programs against the
existing `kafka` EC2 broker (`localhost:9092`, SASL/PLAIN, `bob` user
with READ on the `producer-lab` topic created in lesson 8). Each
program demonstrates one concept from this lecture. We reuse
`producer-lab` and feed it with the lesson 8 producer — the consumer
does not need its own topic.

## Learning objectives

After this lesson you will be able to:

- Explain what a consumer group is and why a single consumer is not
  enough, in terms of scaling, fault tolerance, and independent
  readers.
- Trace exactly what happens on each `poll()` call — which component
  asks which, and where latency comes from.
- Predict how partitions are divided among consumers, and what the
  four assignment strategies actually change.
- Explain when a rebalance happens, what it costs, and how to avoid
  triggering unnecessary ones.
- Choose between auto-commit and manual commit, and explain the
  delivery guarantee each one gives (at-most-once vs at-least-once).
- Write and read basic consumer code: the poll loop, `commitSync` /
  `commitAsync`, graceful shutdown with `wakeup()`, and manual
  `assign` / `seek`.

---

## 1. A consumer is a member of a group

Start with the single most important idea, the mirror of last
lesson's "`send()` does not send":

> **The unit Kafka distributes to consumers is the partition, not the
> message.**
>
> The broker never hands out individual records round-robin to
> whoever is free. It assigns whole partitions to consumers, and each
> consumer reads its partitions start to finish.

A **consumer group** is a set of consumers that share the same
`group.id`. Kafka treats them as one logical reader and splits the
topic's partitions between them:

```
   topic: producer-lab                ConsumerGroup (group.id = lesson9)
   ┌──────────────────┐               ┌────────────────────────────┐
   │  partition 0 ────┼─────────────► │  Consumer C1               │
   │  partition 1 ────┼─────────────► │  Consumer C2               │
   │  partition 2 ────┼─────────────► │  Consumer C3               │
   └──────────────────┘               └────────────────────────────┘

        one partition is read by exactly one consumer in the group
        one consumer may read several partitions
```

Two rules follow from "partition is the unit":

1. **A partition is owned by exactly one consumer in the group.** Two
   consumers in the same group never read the same partition. That is
   how a group reads a topic once, not N times.

2. **The parallelism ceiling is the partition count.** A topic with
   3 partitions supports at most 3 working consumers in one group. A
   4th consumer in that group sits **idle** — connected, alive, but
   holding no partition. It is a standby: if a working consumer dies,
   the idle one picks up its partitions.

### Why a group at all?

A single consumer reading all partitions works until the volume grows
past what one process can handle. You cannot just start a second
independent consumer — two unrelated consumers each read *everything*,
so every record is processed twice.

The group is the mechanism that lets you add consumers **without**
duplicating work. Same `group.id` tells the broker "these are one
team, divide the partitions among them". This buys three things a lone
consumer does not have:

- **Scale** — more consumers read more partitions in parallel. To go
  faster, add a consumer (up to the partition count).
- **Fault tolerance** — if a consumer dies, its partitions are
  reassigned to the survivors (a **rebalance**, §4). Reading does not
  stop.
- **Independent readers** — different groups reading the same topic
  each keep their own position. A billing service and an analytics
  service consume `producer-lab` from separate groups, at their own
  pace, without affecting each other.

```
   Two groups, same topic, independent progress:

   topic: producer-lab
   ┌──────────────┐        group "billing"    ── reads, commits its own offsets
   │ p0  p1  p2   │───┐
   └──────────────┘   └──► group "analytics"  ── reads, commits its own offsets
```

`Ex1Consumer` in the lab is a single consumer reading all three
partitions. `Ex2Consumer` is the same code run as several processes in
one group so you can watch the partitions split.

---

## 2. The poll loop: who asks whom, and where latency hides

Consumer code is a loop around one call, `poll()`:

```java
while (running) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
    for (ConsumerRecord<String, String> record : records) {
        process(record);
    }
}
```

`poll()` looks innocent, but it is where all the machinery runs. Here
is the full picture of who talks to whom:

```
   YOUR CODE           KafkaConsumer               BROKER
   (main thread)       (fetcher, runs in poll)     (+ group coordinator)
       │                     │                          │
       │  poll(timeout) ────►│                          │
       │                     │── fetch request ────────►│  "records from p0,p1,p2
       │                     │                          │   starting at offset N"
       │                     │                          │  waits: fill fetch.min.bytes
       │                     │                          │  ...but not past fetch.max.wait.ms
       │                     │◄── records ──────────────│
       │◄── ConsumerRecords ─│                          │
       │                     │                          │
       │  for (record: ...)  │   ◄── YOUR processing    │
       │  { process(...) }   │                          │
       │                     │                          │
       │  commitSync() ─────►│── commit offset ────────►│  writes to __consumer_offsets
       │                     │◄── ok ───────────────────│
       │◄── returns ─────────│                          │

   IN THE BACKGROUND, a separate heartbeat thread (not about data):
       heartbeat ──────────► group coordinator     every heartbeat.interval.ms (~3s)
       no heartbeat for session.timeout.ms (45s) → member is dead → rebalance
```

The three questions being asked:

- **your code → consumer:** `poll()` — "give me a batch of records".
- **consumer → broker:** fetch — "send records from my partitions
  starting at offset N".
- **consumer → broker:** commit — "remember I have processed up to
  offset N" (written to the internal `__consumer_offsets` topic).

Heartbeats run on their own thread, so a slow `poll()` does not
immediately look dead — but see the `max.poll.interval.ms` trap below.

### Where the latency comes from

This flow is exactly where "why is my consumer slow / laggy / getting
kicked out" questions live:

1. **Empty or slow `poll()`.** When little data is available, the
   broker holds the fetch response until `fetch.max.wait.ms`
   (default 500 ms) trying to accumulate `fetch.min.bytes`. Not a
   bug — this is read-side batching. Set `fetch.min.bytes=1` (the
   default) for low latency, higher for throughput.

2. **First `poll()` after `subscribe()`.** The consumer must first
   join the group and be assigned partitions (a rebalance). There is
   a pause before any data arrives on the very first poll.

3. **Slow processing loop.** While your `for` loop runs, the next
   `poll()` is not called. If processing a batch of `max.poll.records`
   (default 500) takes a long time, lag grows — and if it takes
   longer than **`max.poll.interval.ms` (default 5 min)**, the broker
   assumes the consumer is dead and rebalances its partitions away.
   Reading stops. This is the classic production failure.

4. **`commitSync()` blocks.** It waits for the broker to acknowledge
   the offset write. Committing synchronously after every batch adds a
   round-trip each time; this is why `commitAsync` exists (§6).

5. **`earliest` on a large topic.** With no committed offset and
   `auto.offset.reset=earliest`, the first run reads the entire topic
   from offset 0 — that can take a while.

6. **Too few partitions.** One partition = one consumer. Three
   partitions cap the group at three working consumers; a fourth adds
   no throughput.

> **The rule that ties it together:** `poll()` must be called
> regularly. A consumer that stops calling `poll()` — because it is
> stuck processing — is treated as dead and loses its partitions.
> Keep per-batch processing shorter than `max.poll.interval.ms`, or
> lower `max.poll.records` so each batch is smaller.

---

## 3. How partitions are divided: assignment strategies

When a group forms or changes, one member (the group leader) computes
who gets which partitions, using the strategy in
`partition.assignment.strategy`. There are four:

| Strategy | What it does |
|---|---|
| `RangeAssignor` | Per topic, splits partitions into contiguous ranges and hands one range to each consumer. Default (see note). |
| `RoundRobinAssignor` | Lays all partitions of all subscribed topics in a line and deals them out one by one. Evenest spread. |
| `StickyAssignor` | Like round-robin for balance, but on a rebalance tries to keep each consumer's existing partitions to minimise movement. |
| `CooperativeStickyAssignor` | Same balancing as sticky, but uses the **cooperative** (incremental) rebalance protocol — see §4. |

> **Correction to a common slide.** The default is not "RangeAssignor"
> alone — since Kafka 2.4 the default `partition.assignment.strategy`
> is the list `[RangeAssignor, CooperativeStickyAssignor]`. Range is
> the one actually used; CooperativeSticky is listed so a group can
> migrate to the cooperative protocol by rolling restart without a
> flag day. And RangeAssignor is not "for reading topics in the right
> order" — it gives no cross-topic ordering guarantee. Its one real
> property is **co-partitioning**: because it assigns per topic by the
> same partition ranges, partition *p* of topic A and partition *p* of
> topic B land on the same consumer, which helps when you join two
> co-partitioned topics on the same key. That is a side effect of
> range assignment, not its purpose.

`Ex5` and the lab's assignment step let you run the same group with
different strategies and watch the assignment in the rebalance
listener output.

---

## 4. Rebalancing

A **rebalance** is the process of recomputing partition ownership and
redistributing partitions among the group's members.

### What triggers it

- A consumer **joins** the group (new instance starts).
- A consumer **leaves** — cleanly (`close()`) or by dying (no
  heartbeat for `session.timeout.ms`, or no `poll()` for
  `max.poll.interval.ms`).
- The **subscription changes**, or the number of **partitions**
  in a subscribed topic increases.

It does **not** trigger on `consumer.pause()` — pausing stops fetching
from a partition without leaving the group.

### The coordinator and the timers

One broker acts as the **group coordinator** for a group. Each
consumer sends heartbeats to it:

```
   Kafka cluster                     Consumer group (group.id = lesson9)
   ┌──────────────┐                  ┌──────────────────────────────┐
   │  broker      │◄── heartbeat ────│  C1  (group leader)          │
   │  = group     │◄── heartbeat ────│  C2                          │
   │  coordinator │◄── heartbeat ────│  C3                          │
   └──────────────┘                  └──────────────────────────────┘
         │
         └─ no heartbeat within session.timeout.ms → declare dead → rebalance
```

The relevant timers, with **current** defaults (Kafka 3.x / 4.x):

| Setting | Default | Meaning |
|---|---|---|
| `heartbeat.interval.ms` | 3000 | how often the consumer pings the coordinator |
| `session.timeout.ms` | **45000** | no heartbeat for this long → member is dead |
| `max.poll.interval.ms` | 300000 | no `poll()` for this long → member is dead |

> **Correction to a common slide.** `session.timeout.ms` default is
> **45 s**, not 10 s. The old 10 s default was raised to 45 s in Kafka
> 3.0 (KIP-735) to tolerate transient network/GC pauses without
> needless rebalances. On the 4.3 broker in the lab it is 45 s.

Keep `heartbeat.interval.ms` at roughly ⅓ of `session.timeout.ms`, so
a few missed heartbeats do not immediately look like a dead member.

### Eager vs cooperative

Two rebalance protocols exist:

- **Eager** (classic): every consumer gives up **all** its partitions,
  then the new assignment is computed, then everyone gets partitions
  back. During that window nobody reads — a "stop-the-world" pause.
- **Cooperative** (`CooperativeStickyAssignor`): only the partitions
  that actually need to move are revoked; everything else keeps
  reading. Rebalances become incremental instead of stop-the-world.
  This is why cooperative is the modern recommendation for large
  groups.

### Reacting to a rebalance: `ConsumerRebalanceListener`

You can run code when partitions are taken away or handed over — for
example, commit offsets for partitions about to be revoked, or load
per-partition state:

```java
consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // last chance to commit before losing these partitions
    }
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // new partitions just arrived
    }
});
```

`Ex2Consumer` implements this listener and prints `assigned:` /
`revoked:` so the rebalance is visible when you start and stop
instances.

### Graceful shutdown with `wakeup()`

`poll()` blocks. To stop a consumer cleanly from another thread (a
shutdown hook on Ctrl-C), call `consumer.wakeup()` — it makes the
in-progress `poll()` throw `WakeupException`, which you catch to break
the loop and `close()`:

```java
Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
try {
    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        // process
    }
} catch (WakeupException e) {
    // expected on shutdown
} finally {
    consumer.close();   // leaves the group cleanly → fast rebalance
}
```

`close()` sends an explicit "leaving" message to the coordinator, so
the group rebalances immediately instead of waiting out
`session.timeout.ms`.

### Avoiding unnecessary rebalances

Rebalances are disruptive (reading pauses, stateful consumers must
rebuild state for new partitions). Reduce them by:

- setting `heartbeat.interval.ms ≈ ⅓ session.timeout.ms`;
- giving `max.poll.interval.ms` enough headroom for the slowest batch;
- using **static membership** — set a stable `group.instance.id` per
  instance so a restart within `session.timeout.ms` rejoins with the
  same identity and does **not** trigger a rebalance.

---

## 5. Offsets

An **offset** is the sequential position of a record within a
partition: 0, 1, 2, … Offsets are per partition; there is no global
order across a topic.

Three positions matter for one consumer on one partition:

```
   partition 2:  [0][1][2][3][4][5][6][7][8][9]
                              ▲           ▲     ▲
                              │           │     │
                        committed      current  end
                        offset=4     position=7  offset=10

   lag = end - committed = 10 - 4 = 6 records behind
```

- **current position** — where the next `poll()` will read from.
- **committed offset** — the last offset the consumer has saved as
  "done". After a restart or a rebalance, a consumer resumes from the
  committed offset, not from where it happened to be in memory.
- **end offset** (log-end) — one past the last record. `end −
  committed` is the consumer **lag**, the standard health metric.

### Where committed offsets live

Committed offsets are stored in an internal compacted topic,
`__consumer_offsets`, keyed by (group, topic, partition). This is why
a group resumes where it left off, and why two groups are independent:
they write different keys.

The value committed is **the next offset to read** (committed + 1
relative to the last processed record). A common off-by-one confusion
— committing offset 5 means "I have processed through 4, start me at
5 next time".

### `auto.offset.reset`

When a consumer has **no** committed offset for a partition (brand-new
group, or the old offset has aged out), this setting decides where to
start:

| Value | Behaviour |
|---|---|
| `latest` (default) | start at the end — read only records produced from now on |
| `earliest` | start at offset 0 — read the whole partition history |
| `none` | throw an exception; make the caller decide |

> The default is `latest`, which surprises people: a fresh consumer
> group started after the data was produced reads **nothing** until
> new records arrive. For labs and for "process everything" services,
> set `earliest`. The lab's `Utils` sets `earliest` so the exercises
> see the messages the lesson 8 producer already wrote.

---

## 6. Committing offsets

Committing is how a consumer records its progress. The choice of
*when* and *how* to commit is the choice of delivery guarantee.

### Auto-commit

With `enable.auto.commit=true` (the default), the consumer commits the
latest polled offsets automatically, at most once every
`auto.commit.interval.ms` (default 5000 ms), during `poll()`.

Simple, but the commit is tied to `poll()`, not to your processing:

- If you commit (poll) **before** finishing processing and then crash,
  those records are marked done but were never fully processed →
  **at-most-once**, possible loss.
- In practice auto-commit commits what was returned by the previous
  poll on the next poll, so a crash mid-batch can also **reprocess**
  the current batch → duplicates.

Auto-commit is fine for tolerant workloads (metrics, logs). For
anything where each record must be handled exactly once on your side,
turn it off and commit manually.

### Manual commit

Set `enable.auto.commit=false` and commit yourself, **after**
processing, so "committed" always means "actually handled". This is
**at-least-once**: a crash after processing but before committing
reprocesses the batch, never loses it.

Two methods:

- **`commitSync()`** — blocks until the broker acknowledges, retries
  on retriable errors. Safe, slower.
- **`commitAsync()`** — fires the commit and returns immediately,
  optionally with a callback for the result. Fast, but does not retry
  (a later commit supersedes it anyway).

```java
// commitSync: process the batch, then commit and wait
for (ConsumerRecord<String, String> r : records) process(r);
consumer.commitSync();
```

```java
// commitAsync with a callback
consumer.commitAsync((offsets, exception) -> {
    if (exception != null) log.warn("commit failed: {}", exception.getMessage());
});
```

### The hybrid pattern

The production idiom, and what `Ex4Consumer` shows: `commitAsync` in
the loop (fast, non-blocking on the hot path), and one `commitSync` in
a `finally` block to guarantee the final offsets are durable before
the consumer closes.

```java
try {
    while (running) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> r : records) process(r);
        consumer.commitAsync();          // fast path
    }
} finally {
    try {
        consumer.commitSync();           // last commit must land
    } finally {
        consumer.close();
    }
}
```

`Ex3Consumer` is the plain `commitSync` version; `Ex4Consumer` is the
hybrid. Exactly-once end-to-end (consume → produce as one atomic unit)
needs transactions, which is lesson 11.

---

## 7. All the configuration knobs

Reference table. Sections above are cited by number.

| Setting | Default | What it does |
|---|---|---|
| `bootstrap.servers` | — | broker(s) to contact first; the rest is discovered. |
| `key.deserializer`, `value.deserializer` | — | turn `byte[]` back into your key/value objects. Must match the producer's serializers. |
| `group.id` | null | the group this consumer joins. Required for group reads and offset commits. §1 |
| `enable.auto.commit` | true | commit offsets automatically. §6 |
| `auto.commit.interval.ms` | 5000 | how often auto-commit fires. §6 |
| `auto.offset.reset` | `latest` | where to start with no committed offset. §5 |
| `partition.assignment.strategy` | `[Range, CooperativeSticky]` | how partitions are divided. §3 |
| `heartbeat.interval.ms` | 3000 | heartbeat frequency to the coordinator. §4 |
| `session.timeout.ms` | 45000 | miss heartbeats this long → dead. §4 |
| `max.poll.interval.ms` | 300000 | miss `poll()` this long → dead. §2, §4 |
| `group.instance.id` | null | static membership; restart without rebalance. §4 |
| `max.poll.records` | 500 | max records returned by one `poll()`. §2 |
| `max.partition.fetch.bytes` | 1048576 (1 MiB) | max data per fetch **per partition**. |
| `fetch.min.bytes` | 1 | broker waits to accumulate this much before replying. |
| `fetch.max.wait.ms` | 500 | …but no longer than this. §2 |
| `fetch.max.bytes` | 52428800 (50 MiB) | max data per fetch across all partitions. |
| `isolation.level` | `read_uncommitted` | `read_committed` hides aborted-transaction records (lesson 11). |

### The ones worth extra attention

**`max.poll.records` and `max.poll.interval.ms` are a pair.** If each
record is expensive to process (a DB write, an external API call),
500 records per poll can blow past the 5-minute poll interval and get
the consumer evicted. The fix is to lower `max.poll.records` (to 50 or
100) and/or raise `max.poll.interval.ms`. Tune them together.

**`fetch.min.bytes` / `fetch.max.wait.ms` are read-side batching.**
Raising `fetch.min.bytes` trades latency for throughput exactly like
`linger.ms` did on the producer: the broker waits to send a fuller
response.

**`isolation.level`** only matters once producers use transactions.
`read_committed` makes the consumer skip records from aborted
transactions and not read past any open transaction. Default is
`read_uncommitted` (read everything).

---

## 8. Writing consumer code

A complete, correct consumer:

```java
package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class SimpleConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                  StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                  StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "lesson9");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));

        consumer.subscribe(List.of("producer-lab"));

        try {
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("key=%s value=%s partition=%d offset=%d%n",
                            record.key(), record.value(),
                            record.partition(), record.offset());
                }
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            // expected on shutdown
        } finally {
            consumer.close();
        }
    }
}
```

### Line by line

**Config.** The deserializers must match what the producer wrote —
`producer-lab` was written with `StringSerializer`, so we read with
`StringDeserializer`. `group.id` puts this consumer in a group.
`auto.offset.reset=earliest` reads existing data. `enable.auto.commit
=false` means we commit ourselves.

**`subscribe(List.of(topic))`.** Joins the group and lets Kafka assign
partitions dynamically. You do **not** name partitions here — the
group coordinator does that. `subscribe` can take several topics.

**The poll loop.** `poll(Duration)` returns whatever is available up
to `max.poll.records`, waiting up to the duration if nothing is ready.
Never do slow work between polls beyond `max.poll.interval.ms`.

**`commitSync()` after the loop body.** Commit after processing, so
committed always means handled → at-least-once.

**`wakeup()` + `WakeupException` + `close()`.** The graceful shutdown
path from §4.

### subscribe vs assign

`subscribe()` is the group path: dynamic assignment, rebalances,
committed offsets. For full manual control there is `assign()`: you
name the exact partitions yourself, there is no group coordination and
no automatic rebalance. Combined with `seek()` it lets you read from a
precise offset:

```java
TopicPartition p0 = new TopicPartition("producer-lab", 0);
consumer.assign(List.of(p0));
consumer.seekToBeginning(List.of(p0));   // replay partition 0 from offset 0
```

This is what `Ex5Consumer` does — replay one partition from the start,
ignoring any committed offset, to show that an offset is just a
position. Use `assign`/`seek` for tools, reprocessing, and exact
replay; use `subscribe` for normal group consumption.

---

## 9. Summary

A consumer reads by **partition**, and partitions are divided among
the members of a **consumer group** (same `group.id`). One partition
is read by exactly one consumer in the group; the parallelism ceiling
is the partition count; extra consumers stand idle. Groups give scale,
fault tolerance, and independent readers.

**The poll loop** drives everything: fetch, delivery to your code, and
(if manual) commit. Heartbeats run on a background thread, but a
consumer that stops calling `poll()` longer than
`max.poll.interval.ms` is declared dead and loses its partitions. Most
consumer latency traces to fetch batching, slow processing loops, or
`earliest` on a big topic.

**Assignment strategies** decide how partitions are split: Range
(default, per-topic), RoundRobin (evenest), Sticky (minimal movement),
CooperativeSticky (incremental rebalance). The default is
`[Range, CooperativeSticky]`.

**Rebalances** happen on join/leave, subscription change, or partition
count change. Current defaults: `session.timeout.ms=45000`,
`heartbeat.interval.ms=3000`, `max.poll.interval.ms=300000`. Reduce
rebalances with the ⅓ heartbeat ratio, enough poll headroom, and
static membership (`group.instance.id`). Shut down with `wakeup()`.

**Offsets** are per-partition positions stored in `__consumer_offsets`
per group. Committed = next offset to read. `auto.offset.reset`
defaults to `latest` (surprising) — use `earliest` to read history.

**Commit strategy = delivery guarantee.** Auto-commit is at-most-once
in practice; manual commit after processing is at-least-once. The
hybrid pattern is `commitAsync` in the loop plus `commitSync` in
`finally`. Exactly-once needs transactions (lesson 11).

---

## References

### Official documentation

| Source | URL |
|---|---|
| Apache Kafka Consumer Configs | https://kafka.apache.org/documentation/#consumerconfigs |
| `KafkaConsumer` JavaDoc | https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html |
| Consumer design / group protocol | https://kafka.apache.org/documentation/#design_consumerposition |

### KIPs that shifted defaults over time

| KIP | Change | Kafka version |
|---|---|---|
| KIP-429 | Cooperative (incremental) rebalance protocol | 2.4 |
| KIP-735 | `session.timeout.ms` default 10 s → 45 s | 3.0 |
| KIP-848 | New consumer group protocol (broker-side assignment) | 3.7+ (preview) |

### Book

| Source | URL |
|---|---|
| Kafka: The Definitive Guide, 2nd ed. (Shapira, Palino, Sivaram, Petty), Chapter 4 | https://www.confluent.io/resources/kafka-the-definitive-guide-v2/ |

### Versions used in the lab

- Broker on `kafka` EC2: Apache Kafka 4.3.0 (native install, KRaft single-node)
- Client library in `consumer-java/build.gradle`: `kafka-clients` 3.7.0
- Topic `producer-lab` (3 partitions) reused from lesson 8; consumer runs as `bob` (READ)
