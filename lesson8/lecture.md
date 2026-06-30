# Lesson 8 — Kafka Producer API

In the previous lessons we set up a Kafka cluster, configured
security, and learned how to manage topics and ACLs declaratively
through Terraform. The cluster is now ready to receive messages.

This lesson is about **the other side**: the client that actually
sends messages to Kafka. We will look inside the Java `KafkaProducer`
class — how it works internally, what settings it has, and how to
write correct producer code.

The companion lab (`LAB.md`) runs five small Java programs against
the existing `kafka` EC2 broker (172.31.29.117:9092, SASL/PLAIN,
alice user with WRITE ACL on a dedicated `producer-lab` topic). Each
program demonstrates one concept from this lecture.

## Learning objectives

After this lesson you will be able to:

- Explain step by step what happens between `producer.send(record)`
  and the message landing in a partition on disk.
- Predict which partition a record will be written to, given the
  record's key and partition fields.
- Choose the right value of `acks` for a given durability requirement
  and explain why `acks=all` alone is not enough.
- Configure an idempotent producer and explain what guarantee
  idempotence provides — and what it does not provide.
- Write and read basic producer code in Java, understanding the
  callback execution model, `flush()`, and `close()`.
- Reason about `linger.ms`, `batch.size`, and `delivery.timeout.ms`
  as a single coupled system, not as independent knobs.

---

## 1. What happens when you call `send()`

Before we look at any code, here is the most important idea in the
whole lesson:

> **`producer.send(record)` does not send anything over the network.**
>
> It only puts the record into a queue inside the producer process.
> A separate thread sends batches of records over the network later.

This is the source of almost every "but my message did not arrive"
question, and once you understand the picture below, the behavior of
`flush()`, `close()`, `linger.ms`, and `batch.size` all make sense
as one system.

### The picture

```
   ┌──────────────────────────────────────────────────────┐
   │                Your application (JVM)                │
   │                                                       │
   │   Your code               KafkaProducer object        │
   │   ─────────               ────────────────────        │
   │                                                       │
   │   send(record1) ────────► queue for partition 0       │
   │                           ┌──────────────────┐        │
   │                           │ record1, record4 │        │
   │                           └──────────────────┘        │
   │                                                       │
   │   send(record2) ────────► queue for partition 1       │
   │                           ┌──────────────────┐        │
   │                           │ record2, record5 │        │
   │                           └──────────────────┘        │
   │                                                       │
   │   send(record3) ────────► queue for partition 2       │
   │                           ┌──────────────────┐        │
   │                           │ record3          │        │
   │                           └──────────────────┘        │
   │                                                       │
   │                                  ▲                    │
   │                                  │ reads queues       │
   │                                  │ packs into batches │
   │                                  │ sends over network │
   │                                                       │
   │                           ┌──────────────────┐        │
   │                           │  Sender thread   │        │
   │                           └────────┬─────────┘        │
   │                                    │                  │
   └────────────────────────────────────┼──────────────────┘
                                        │
                                        ▼
                          ┌─────────────────────────┐
                          │     Kafka broker        │
                          │                         │
                          │  topic "producer-lab"   │
                          │    partition 0          │
                          │    partition 1          │
                          │    partition 2          │
                          └─────────────────────────┘
```

There are **two threads**:

1. **Your application thread** — the one you write code on. When you
   call `send(record)`, this thread does only one thing: it puts the
   record into the right in-memory queue and returns.

2. **The Sender thread** — a background thread that `KafkaProducer`
   starts for you. This thread watches the queues, collects records
   into **batches**, opens TCP connections to brokers, and sends the
   batches over the network.

There is **one queue per partition** of every topic this producer
writes to. If your producer writes to one topic with 3 partitions,
the producer has 3 queues internally. If it writes to two topics with
3 partitions each, the producer has 6 queues internally.

### Why this design?

Imagine sending 10,000 messages per second. The naive design — one
TCP request per `send()` — would mean 10,000 TCP round-trips per
second to the broker. Each round-trip costs network bandwidth, broker
CPU (parsing the request, locking the partition, writing to disk),
and producer CPU.

With batching, the producer collects 100 messages, sends them in one
TCP request, and the broker processes them all together. Same 10,000
messages per second becomes only 100 requests per second. Network
bandwidth drops by ~100x. Broker CPU drops dramatically. Producer
CPU drops too.

This is why **batching is the foundation of Kafka throughput**, not
an optimization layered on top. The whole producer is built around
the idea of batching records before sending.

### The four knobs that control batching

Now we can see why these four settings exist and how they relate:

| Setting | What it controls |
|---|---|
| `batch.size` | "Don't make a batch bigger than this many bytes." Default: 16 KiB (16384 bytes). When a batch is full, Sender ships it right away. |
| `linger.ms` | "Wait this long before shipping a partial batch." Default: 0 ms — ship as soon as you can. |
| `flush()` | "Sender, drain everything in the queues right now and don't return to me until you're done." |
| `close()` | "Sender, do a `flush()` and then stop running." |

The default `linger.ms=0` means: Sender does not wait at all. The
moment one record is in the queue, Sender starts trying to send it.
This is great for latency, bad for throughput, because batches stay
small.

A production setting of `linger.ms=20` means: Sender waits up to
20 ms hoping more records will arrive that it can batch together.
This trades a little latency (up to 20 ms extra) for much better
throughput.

### What this means for your code

> **If you call `send()` and then exit the JVM, your message is most
> likely lost.**

Why? Because `send()` only put the record in a queue. Sender was
about to send it but the JVM exited before that happened.

This is what `close()` is for. `close()` waits for the Sender thread
to finish sending everything in the queues before letting the JVM
exit. The cleanest way to ensure this is `try-with-resources`:

```java
try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
    producer.send(record1);
    producer.send(record2);
    // try-with-resources automatically calls producer.close() here
}
```

When the `try` block exits, Java calls `producer.close()` for you.
`close()` flushes the queues and waits up to a long time for
everything to be delivered before returning.

`flush()` does the same thing but does not stop the producer. Use it
in the middle of long-running code when you want to be sure
everything sent so far has been delivered, but you still want to keep
sending more records afterwards.

---

## 2. What a record looks like: `ProducerRecord`

The object you pass to `send()` is a `ProducerRecord<K, V>`. Here is
what one actually looks like in code:

```java
ProducerRecord<String, String> record = new ProducerRecord<>(
    "user-events",                            // topic
    null,                                     // partition (optional)
    System.currentTimeMillis(),               // timestamp (optional)
    "user-42",                                // key
    "{\"action\":\"login\",\"ip\":\"1.2.3.4\"}"  // value
);
```

All the fields:

| Field | Required? | What it is |
|---|---|---|
| `topic` | yes | The topic name. Must exist on the broker, or the broker must have `auto.create.topics.enable=true` (usually `false` in production). |
| `value` | yes (but can be `null`) | The actual message data, as an object that the value serializer knows how to turn into bytes. |
| `key` | no | Used for partition selection (next section) and for log compaction. |
| `partition` | no | If you set this, the producer sends to exactly this partition and skips the partitioner. |
| `timestamp` | no | A Unix millisecond timestamp. If you don't set it, the producer uses `System.currentTimeMillis()`. |
| `headers` | no | A map of key-value metadata, like HTTP headers. Used for tracing IDs, schema IDs, routing info, etc. |

### Special cases worth knowing

**A `null` value is meaningful.** In a regular topic, a `null` value
is just a record with empty content. In a **compacted** topic,
sending a record with `key="user-42"` and `value=null` is called a
**tombstone** — it tells Kafka "delete the latest value for this
key after compaction runs". This is how compacted topics support
deletion.

**Timestamps interact with topic settings.** The broker has a setting
`message.timestamp.type` per topic:

- `CreateTime` (default) — the broker keeps whatever timestamp the
  producer sent. If the producer left it blank, the producer's
  current wall clock is used.
- `LogAppendTime` — the broker overwrites whatever the producer sent
  with the time the message was written to disk.

If you ever need to back-fill historical data (load yesterday's
events into Kafka today), use `CreateTime` and set the timestamps
explicitly on each `ProducerRecord`. Otherwise Kafka will think
everything happened "now".

**Headers are new-ish.** They were added in Kafka 0.11. The most
common use today is distributed tracing — propagating the W3C
`traceparent` header so spans in your monitoring system link
producer-side and consumer-side processing. You can ignore them for
this lab, but they will reappear when we add OpenTelemetry later in
the course.

---

## 3. Which partition does my message go to?

A topic has several partitions, and each message goes to exactly
one. **Which one?** This is determined by an algorithm inside the
producer called the **partitioner**.

The algorithm has three steps:

```
Did you set ProducerRecord.partition explicitly?
│
├── YES → use that partition. Done. (skip partitioner)
│
└── NO  → Did you set a key?
         │
         ├── YES → partition = murmur2(key) mod num_partitions
         │
         └── NO  → use the default partitioner
                   (uniform sticky partitioner since Kafka 3.3)
```

Let's go through each case with a real example. Assume our topic
`producer-lab` has 3 partitions (partition 0, 1, and 2).

### Case 1: You set `partition` explicitly

```java
new ProducerRecord<>("producer-lab", 1, "key1", "value1");
//                                  ^
//                                  partition = 1, hardcoded
```

This record goes to partition 1. The partitioner is not consulted.
The key is still recorded with the message but does not affect
routing. Almost no one uses this in practice — it gives up the whole
point of having a partitioner.

### Case 2: You set a key, no explicit partition

```java
new ProducerRecord<>("producer-lab", "user-42", "login event");
//                                   ^
//                                   key = "user-42"
```

The producer computes `murmur2("user-42") mod 3`. `murmur2` is a hash
function that turns the string `"user-42"` into a 32-bit integer.
That integer modulo 3 gives a number in `{0, 1, 2}` — the partition.

The important property: **the same key always hashes to the same
partition**, as long as the number of partitions does not change.

Why this matters: imagine you are tracking user logins. User 42 logs
in, then logs out, then logs in again. Each event is a separate
message. If you set `key = "user-42"` for all three events, they all
go to the same partition. Kafka guarantees order within a partition,
so the consumer will read them in the order: login → logout → login.

If you had not set a key, the three events would scatter across
partitions and the consumer might read them as: logout → login →
login, which is nonsense.

> **Rule of thumb:** if there is a logical entity whose events must
> arrive in order (a user, an order, an account, a session), set the
> key to that entity's ID. If order does not matter (access logs,
> metrics, traces), leave the key as `null`.

### Case 3: No key, no partition

```java
new ProducerRecord<>("producer-lab", null, "access log line");
//                                   ^
//                                   no key
```

When neither is set, the producer uses its default partitioner.
Older documentation describes this as round-robin, but that stopped
being the default in Kafka 2.4 (March 2020).

The current default is the **uniform sticky partitioner**: the
Sender picks one partition and keeps writing records to it until the
batch fills up or `linger.ms` expires. Then it switches to a
different partition for the next batch.

Why is this better than round-robin? Because true round-robin would
produce many tiny batches (one batch per partition per record),
which wastes network capacity. Sticky partitioner produces few large
batches, which is much more efficient.

```
   True round-robin (old behavior, before 2.4):
   ─────────────────────────────────────────────
   record1 ─► partition 0  ─► tiny batch, 1 message
   record2 ─► partition 1  ─► tiny batch, 1 message
   record3 ─► partition 2  ─► tiny batch, 1 message
   record4 ─► partition 0  ─► another tiny batch
   ...

   Sticky partitioner (default since 2.4):
   ───────────────────────────────────────────
   record1 ─► partition 0
   record2 ─► partition 0
   record3 ─► partition 0   one big batch
   record4 ─► partition 0
   record5 ─► partition 0
   (batch fills or linger.ms expires, switch)
   record6 ─► partition 1
   record7 ─► partition 1
   ...
```

The end result is the same — records distribute across partitions
roughly evenly — but the network and broker work is much less.

### The repartitioning trap

If you set keys and then **change the number of partitions later**,
something subtle and bad happens.

Suppose user 42's events have been hashing to partition 1
(`murmur2("user-42") mod 3 == 1`). The topic gets expanded from 3 to
4 partitions. Now the producer computes `murmur2("user-42") mod 4`,
which is a different number — maybe 3.

From this moment on, new events for user 42 go to partition 3, but
all the old events for user 42 are still in partition 1. The
consumer, reading partition by partition, sees them out of order.

This is why production Kafka topics rarely change partition counts
once they have data and consumers. If dynamic sharding is needed,
applications use custom partitioners or consistent hashing on the
producer side. For this lab, we will create the topic once with
3 partitions and leave it alone.

---

## 4. How do I know my message arrived? `acks`

So far we have talked about how messages get from your code into a
queue and from the queue out to the broker. Now: how does the broker
tell the producer "yes I got it"?

This is what `acks` controls. But before we look at the three values,
we need to understand how partitions are stored on the broker.

### Leaders, followers, and ISR

Every partition has one **leader** — a single broker that holds the
authoritative copy. The producer always writes to the leader.

Optionally, a partition has **followers** — other brokers that hold
copies of the same data. Followers pull data from the leader in the
background. This is how Kafka survives broker failures: if the leader
crashes, a follower is promoted to leader.

The set of followers that are currently up-to-date with the leader
is called the **ISR** — In-Sync Replicas. A follower is in the ISR if
it has fetched everything the leader has within the last
`replica.lag.time.max.ms` (default 30 seconds). The leader is always
in its own ISR.

```
   replication.factor = 3, all replicas healthy
   ──────────────────────────────────────────────

       ┌────────────┐
       │  broker A  │   ◄── leader for partition 0
       │ partition 0│
       └─────┬──────┘
             │
       fetches│data
             │
       ┌─────▼──────┐                    ISR = {A, B, C}
       │  broker B  │
       │ partition 0│   ◄── follower
       └─────┬──────┘
             │
             │
       ┌─────▼──────┐
       │  broker C  │   ◄── follower
       │ partition 0│
       └────────────┘
```

### Now `acks`

`acks` tells the broker when to reply with "success":

#### `acks=0` — fire and forget

```
   Producer                  Broker (leader)
      │                            │
      │── send record ────────────►│
      │                            │  (writes or doesn't write,
      │                            │   we don't care)
      │                            │
      │                            │
      │ (producer keeps going,     │
      │  never waits for reply)    │
      │                            │
```

The producer does not wait for any response. If the leader crashes
between receiving the TCP packet and writing the record, the producer
never knows. **Zero durability.**

Use case: metrics, traces, debug logs — anything where occasional
loss is acceptable in exchange for maximum throughput.

#### `acks=1` — leader acknowledged

```
   Producer                  Broker (leader)              Followers
      │                            │                          │
      │── send record ────────────►│                          │
      │                            │── write to local log ──► │
      │◄── ack ────────────────────│                          │
      │                            │                          │
      │                            │ (followers replicate     │
      │                            │  in the background, but  │
      │                            │  producer already moved  │
      │                            │  on)                     │
```

The producer waits for the leader to write the record to its local
log and acknowledge. If the leader then crashes **before followers
replicate**, the record is lost — but the producer thinks it
succeeded.

**Some durability**, but a leader crash at the wrong moment loses
data.

#### `acks=all` — all in-sync replicas acknowledged

```
   Producer                  Broker (leader)              Followers
      │                            │                          │
      │── send record ────────────►│                          │
      │                            │── write to local log     │
      │                            │                          │
      │                            │◄── fetch ────────────────│
      │                            │── send record ──────────►│
      │                            │                          │
      │                            │◄── follower has it ──────│
      │◄── ack ────────────────────│                          │
      │                            │                          │
```

The producer waits until **every replica in the ISR** has the
record. **Maximum durability.** If the leader crashes after the ack,
one of the followers has the record and becomes the new leader.

### The `acks=all` trap

Here is a trap that catches everyone the first time:

> **`acks=all` is meaningless if your partition has only one
> replica.**

Why? Because the ISR is the set of replicas that are in sync.
"All replicas acknowledged" means "every replica in the ISR
acknowledged". If there is only one replica (the leader), then "all"
is just "the leader" — exactly the same as `acks=1`.

This is why our single-broker lab cannot really demonstrate the
difference between `acks=1` and `acks=all`. Ex5 will time them both
and you will see almost identical numbers.

The fix in production is to set, on the topic:

- `replication.factor=3` — write data to 3 brokers
- `min.insync.replicas=2` — require at least 2 to be in sync

With these settings, `acks=all` actually means "at least 2 brokers
have a copy before I confirm to you". If only 1 broker is healthy,
the producer gets `NotEnoughReplicasException` and cannot write —
which is much better than silently writing to a single replica that
might crash next.

This is the **canonical durable setup** for any topic where data
loss is unacceptable:

```
   replication.factor = 3
   min.insync.replicas = 2
   acks = all
```

One broker can fail without affecting writes. Two brokers failing
stops new writes, but no data is lost.

### A side note on fsync

`acks=all` guarantees the record is in the **OS page cache** of all
ISR brokers, not necessarily on disk. The broker calls `fsync()`
(the system call that flushes the page cache to disk) only
periodically — typically not after every record, because that would
be devastatingly slow.

If every broker in the cluster loses power simultaneously, the last
few seconds of accepted writes might be lost. Kafka considers this
an acceptable tradeoff: replication, not fsync, is the durability
mechanism. The probability of every broker in a cluster losing power
at the same time (in different racks or different availability
zones) is much lower than the probability of one broker dying
randomly.

---

## 5. Avoiding duplicates: idempotence

Here is a problem you will hit in production:

**Without idempotence, retries can create duplicates.**

The scenario, step by step:

```
   1. Producer sends a batch to the broker.

   2. The broker writes the batch successfully and starts to send
      back the ack:

         Producer ─── batch ──► Broker  (broker writes batch)

   3. The ack is lost in transit (network glitch, broker process
      paused for GC, switch reset, anything):

         Producer ◄─/ ack ─/─── Broker

   4. The producer waits for a while, decides the request failed,
      and retries:

         Producer ─── batch ──► Broker  (broker writes batch AGAIN)

   5. The broker gets the batch a second time and writes it again.

   Result: each message in the batch is now in the topic TWICE.
```

This is called **at-least-once delivery**: each message arrives at
least once, but maybe more than once. For many use cases this is
unacceptable. If those messages are payment events, you just charged
the customer twice.

### How idempotence fixes this

Set `enable.idempotence=true` and the producer does this:

```
   1. When the producer starts, it asks the broker for a unique
      Producer ID (PID).

         Producer ──── give me a PID ────► Broker
         Producer ◄─── PID = 42         ─── Broker

   2. The producer attaches the PID plus a sequence number to every
      batch:

         Batch { PID=42, seq=0, records=[r1, r2, r3] }
         Batch { PID=42, seq=1, records=[r4, r5] }
         Batch { PID=42, seq=2, records=[r6] }

   3. The broker remembers the highest sequence it has seen per PID
      per partition. If it receives a batch with a sequence it has
      already written, it silently discards the duplicate and replies
      with success as if it had just written.

   4. The producer's retry from the duplicate-ack scenario above
      arrives with PID=42, seq=1. The broker says "I already have
      seq=1, discarding" and replies OK. No duplicate.
```

This gives **exactly-once-per-partition** semantics. Each message
appears in the partition exactly once, no matter how many retries
happened.

### Important: idempotence has been the default since Kafka 3.0

In Kafka 3.0 (September 2021) and later, `enable.idempotence=true`
is the default. You get exactly-once-per-partition for free unless
you explicitly disable it.

Idempotence has prerequisites that the producer enforces at startup:

- `acks=all` is required (without it, idempotence is meaningless)
- `max.in.flight.requests.per.connection <= 5`
- `retries > 0`

Since 3.0 these are set automatically when idempotence is enabled.

### What idempotence is NOT

Idempotence gives a guarantee about **one partition at a time**. It
does not give atomic writes across multiple partitions or topics.

If you need that — for example, write event X to topic A and event
Y to topic B, both or neither — you need **transactions**
(`transactional.id`, `beginTransaction()`, `commitTransaction()`).
That is a much heavier mechanism, covered in lesson 11.

---

## 6. All the configuration knobs

This table is your reference. Settings discussed above are marked
with a section number.

| Setting | Default | What it does |
|---|---|---|
| `bootstrap.servers` | — | The broker(s) the producer contacts first to learn about the cluster. The full broker list is discovered automatically afterwards. |
| `key.serializer`, `value.serializer` | — | Classes that turn your key and value objects into `byte[]`. Must implement `Serializer<T>`. |
| `acks` | `all` (since 3.0) | See §4. |
| `enable.idempotence` | `true` (since 3.0) | See §5. |
| `compression.type` | `none` | Algorithm to compress batches: `none`, `gzip`, `snappy`, `lz4`, `zstd`. |
| `batch.size` | 16384 | See §1. Maximum batch size in bytes. |
| `linger.ms` | 0 | See §1. How long Sender waits to fill a batch. |
| `delivery.timeout.ms` | 120000 | Overall time budget from `send()` to success-or-failure. Includes batching, network, and all retries. |
| `max.in.flight.requests.per.connection` | 5 | How many requests can be "in flight" to one broker at once. |
| `buffer.memory` | 33554432 (32 MiB) | Total memory the producer uses for all its in-memory queues. When full, `send()` blocks. |
| `max.block.ms` | 60000 | How long `send()` is willing to block when the buffer is full or metadata is unavailable. |

### A few of these deserve extra attention

**`compression.type`**. The default `none` is almost always wrong
for text data (JSON, XML, anything human-readable). These payloads
typically compress 3-5x with little CPU cost. `zstd` is the modern
recommendation — best compression ratio, moderate CPU cost. `lz4`
trades worse ratio for faster compression. Compression is applied
**before** the batch is sent and remains on disk in compressed form;
the consumer decompresses on read.

**`delivery.timeout.ms`**. This is the **total** budget for a
record's delivery. It includes the time spent waiting for batching,
the network round-trip, and any retries. The `retries` parameter is
effectively obsolete in modern Kafka — its default is
`Integer.MAX_VALUE`, and the producer keeps retrying until
`delivery.timeout.ms` runs out. So the knob to tune the
durability-vs-latency tradeoff is `delivery.timeout.ms`, not
`retries`.

**`buffer.memory`**. If your application sends much faster than
Kafka can accept, the in-memory queues grow until they hit this
limit. At that point, `send()` starts to **block** the calling
thread until there is space — for up to `max.block.ms`. After
that, `send()` throws an exception. This is the producer's
back-pressure mechanism.

---

## 7. Writing producer code

Here is a complete, working producer in Java:

```java
package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class SimpleProducer {

    public static void main(String[] args) {
        // Step 1: build the configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                  StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Step 2: construct the producer in try-with-resources
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // Step 3: build and send records
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record =
                    new ProducerRecord<>("producer-lab", "user-" + i, "hello " + i);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.printf("sent partition=%d offset=%d%n",
                            metadata.partition(), metadata.offset());
                    }
                });
            }

            // Step 4: close happens automatically when try block exits
        }
    }
}
```

### Line by line

**Step 1: `Properties props = new Properties()`**

A `Properties` object is just a key-value map of configuration. We
put string keys and string values into it. The keys are documented
in `ProducerConfig` — using the constants there
(`ProducerConfig.BOOTSTRAP_SERVERS_CONFIG`) instead of literal
strings (`"bootstrap.servers"`) gives you compile-time checking. If
you write `"booststrap.servers"` (typo), Kafka silently ignores the
setting because it doesn't recognize the key, and you spend an hour
debugging why your config doesn't take effect. Using the constants
prevents that.

**Step 2: `try (KafkaProducer<String, String> producer = ...)`**

The generic types `<String, String>` say "this producer's keys are
Strings and its values are Strings". They must match the
`key.serializer` and `value.serializer` we set in the config. If you
write `<Integer, String>` but the serializer is `StringSerializer`,
the code compiles but throws `ClassCastException` at runtime.

`try-with-resources` ensures `close()` is called even if an
exception is thrown. Without it, you risk losing records that were
still in the queue.

**Step 3: `producer.send(record, callback)`**

This puts the record in the queue and returns immediately. The
callback is a `BiConsumer` (a function taking two arguments) that
the Sender thread will call later when this record's batch is
acknowledged.

The callback always receives exactly one non-null argument:

- On success: `metadata != null`, `exception == null`. The `metadata`
  tells you which partition and offset the record ended up at.
- On failure: `metadata == null`, `exception != null`. The exception
  tells you what went wrong (timeout, authorization denied, broker
  unreachable, etc.).

Two important rules about callbacks:

1. **Do not block in the callback.** The callback runs on the
   Sender thread. While it runs, Sender is not sending other
   batches. If you block (sleep, do a slow database call, etc.),
   you slow down the whole producer.

2. **Never call `producer.close()` from inside a callback.** This
   deadlocks: `close()` waits for Sender to finish, but Sender is
   busy running your callback.

**Step 4: implicit `close()`**

When the `try` block exits, Java calls `producer.close()` for you.
`close()` waits for Sender to drain all queues before returning.
This is why your messages do not get lost.

### Synchronous sending

If you want `send()` to block until the broker responds, use the
`Future` it returns:

```java
RecordMetadata metadata = producer.send(record).get();  // blocks
System.out.println("got partition " + metadata.partition());
```

This is much slower — thousands of records per second instead of
hundreds of thousands — because each `send()` waits for a full
round-trip. Use it only when you really need the result on the next
line of code, or when volume is too low to bother with async.

### The hybrid pattern for batch jobs

For a batch job that processes a list and then exits:

```java
try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
    for (var event : events) {
        producer.send(new ProducerRecord<>("topic", event.key(), event.value()));
    }
    producer.flush();  // wait for everything to be delivered
    // at this point, either everything succeeded or some callbacks
    // recorded errors that you need to handle
}
```

`flush()` is the explicit "drain the queues now" call. After it
returns, you know everything sent before it has been either
acknowledged or has failed.

---

## 8. Summary

The producer is asynchronous. `send()` only enqueues records; a
background **Sender thread** sends batches over the network. Without
`flush()` or `close()`, records can sit in the queue when the JVM
exits. Use `try-with-resources`.

**Partition selection algorithm:**
- Explicit `partition` field → use it.
- Otherwise, key set → `murmur2(key) mod num_partitions`.
- Otherwise → uniform sticky partitioner (since Kafka 3.3).

The same key always lands in the same partition. Use keys when
ordering of events for a single entity matters.

**Acks:**
- `0` = no durability, max throughput.
- `1` = leader confirmed, but loses data if leader crashes before
  followers replicate.
- `all` = every ISR replica confirmed.

`acks=all` is meaningless without `min.insync.replicas >= 2` and
`replication.factor >= 3`. On a single-broker cluster, `acks=all` is
equivalent to `acks=1`.

**Idempotence** (default since 3.0) gives exactly-once-per-partition
delivery automatically. Atomic writes across multiple partitions
require transactions (lesson 11).

**Tuning knobs:** `linger.ms` and `batch.size` control batching.
`delivery.timeout.ms` is the overall budget. `retries` is effectively
obsolete; tune `delivery.timeout.ms` instead.

The default `linger.ms=0` means "do not optimize for throughput at
all". Production deployments typically set 5-50 ms.

---

## References

### Official documentation

| Source | URL |
|---|---|
| Apache Kafka Producer Configs | https://kafka.apache.org/documentation/#producerconfigs |
| `KafkaProducer` JavaDoc | https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html |
| KIP-98 (Idempotent Producer + Transactions) | https://cwiki.apache.org/confluence/display/KAFKA/KIP-98 |

### KIPs that shifted defaults over time

| KIP | Change | Kafka version |
|---|---|---|
| KIP-91 | `delivery.timeout.ms` introduced; `retries` deprecated as a count | 2.1 |
| KIP-480 | Default partitioner: round-robin → sticky | 2.4 |
| KIP-679 | `enable.idempotence=true` as default | 3.0 |
| KIP-794 | Default partitioner: sticky → uniform sticky | 3.3 |

### Book

| Source | URL |
|---|---|
| Kafka: The Definitive Guide, 2nd ed. (Shapira, Palino, Sivaram, Petty), Chapter 3 | https://www.confluent.io/resources/kafka-the-definitive-guide-v2/ |

### Versions used in the lab

- Broker on `kafka` EC2: Apache Kafka 4.3.0 (native install, KRaft single-node)
- Client library in `producer-java/build.gradle`: `kafka-clients` 3.7.0
- Wire protocol is compatible across these versions
