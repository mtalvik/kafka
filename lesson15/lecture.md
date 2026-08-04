# Lesson 15 — Kafka Streams

The previous lessons built the low-level clients by hand: a Producer that
sends records, a Consumer that polls and commits offsets, and around them
transactions, schemas, and admin plumbing. Every non-trivial pipeline you
write on top of those clients ends up re-implementing the same machinery:
poll a batch, transform it, produce the result to another topic, manage a
consumer group, keep some local aggregate, restore that aggregate after a
crash. Kafka Streams is the library that packages that machinery.

It is a **library, not a cluster**. There is no Streams server to deploy.
You add `kafka-streams` to your application, describe a processing
topology, and start it inside your own process. Under the hood it is built
directly on the Producer and Consumer you already know — there is no magic
layer, just a Consumer feeding a graph of processors that feed a Producer,
plus a state and restore mechanism layered on Kafka topics.

## Where it sits

Streaming frameworks trade off raw power against operational simplicity.
Spark and Flink are separate clusters you run and feed jobs to — maximum
power, maximum operational weight. A hand-written Kafka Consumer is the
opposite: trivial to run, but you build everything yourself. Kafka Streams
sits in the useful corner: close to the Consumer in simplicity (it is just
a library in your service), but with enough built-in power — stateful
operations, joins, windowing, exactly-once — to cover most stream-processing
needs without standing up a second cluster.

Rule of thumb: reach for Streams when the work lives naturally *inside* a
Kafka-native service and the input and output are Kafka topics. Reach for
Flink/Spark when you need a shared job cluster, non-Kafka sources/sinks at
scale, or very large windowed state across many jobs.

## Topology: the processing graph

A Streams application is a **topology** — a directed acyclic graph (DAG) of
processor nodes. Three node roles:

- **Source processor** — reads from one or more input topics, feeds records
  into the graph. No parent.
- **Stream processor** — an intermediate node: filter, map, join, aggregate.
  One or more parents, one or more children.
- **Sink processor** — writes records to an output topic. No children.

Because it is a DAG and not a straight chain, one record can fan out to
several branches (write to a topic *and* print *and* feed a downstream
aggregation), and several streams can merge into one node.

```
              src-topic
                  |
             [ source ]
                  |
             [ toUpper ]-----> [ print ]   (side branch, terminal)
                  |
              [ sink ]
                  |
              out-topic
```

You describe this graph with the DSL (`StreamsBuilder`, `KStream`,
`KTable`), or at the lowest level with the Processor API. The DSL compiles
down to a Processor-API topology.

## Two APIs: DSL and Processor API

The **DSL** is the high-level, declarative surface: `filter`, `mapValues`,
`join`, `groupByKey().count()`. It is what you use 95% of the time.

The **Processor API** is the low-level surface: you implement a `Processor`
(or `FixedKeyProcessor`), get a `ProcessorContext`, and `forward` records
manually. Use it when the DSL cannot express what you need — custom state
access, punctuation on a timer, forwarding to specific children.

> **Correction to a common slide.** Older material introduces the low-level
> node via `Transformer` / `ValueTransformer` and `stream.transform(...)` /
> `transformValues(...)`. **That API is removed in Kafka 4.0** (deprecated
> in 3.3, KIP-820). It will not compile against a 4.x client. Use the
> `org.apache.kafka.streams.processor.api` package instead:
>
> - `process(ProcessorSupplier, storeNames...)` — full `Processor<KIn,VIn,KOut,VOut>`,
>   may change the key by forwarding `record.withKey(...)`.
> - `processValues(FixedKeyProcessorSupplier, storeNames...)` —
>   `FixedKeyProcessor<KIn,VIn,VOut>`, value only, key preserved (so it does
>   **not** force a repartition — see the joins section).
>
> Minimal value-only processor:
>
> ```java
> stream.processValues(() -> new FixedKeyProcessor<String, String, String>() {
>     public void process(FixedKeyRecord<String, String> record) {
>         context.forward(record.withValue(record.value().toUpperCase()));
>     }
>     private FixedKeyProcessorContext<String, String> context;
>     public void init(FixedKeyProcessorContext<String, String> ctx) { this.context = ctx; }
> });
> ```

## Serdes

Every record crossing a topic boundary — input, output, or an internal
changelog/repartition topic — must be serialized. Streams works in terms of
a **Serde**: a paired `Serializer` + `Deserializer` for one type.
`Serdes.String()`, `Serdes.Integer()`, `Serdes.Long()`, `Serdes.ByteArray()`
cover the primitives; you supply your own for domain objects (a JSON or
Avro/Schema-Registry Serde, tying back to lesson 11).

Serdes are supplied in two places: the default key/value Serde in
`StreamsConfig` (`DEFAULT_KEY_SERDE_CLASS_CONFIG` /
`DEFAULT_VALUE_SERDE_CLASS_CONFIG`), and per-operation overrides via
`Consumed.with(...)`, `Produced.with(...)`, `Grouped.with(...)`,
`Materialized.with(...)`, `Joined.with(...)`. When an operation changes the
key or value type, pass the matching Serde explicitly at that operation —
relying on the default is the most common source of a
`ClassCastException` at runtime.

## Stateless operations

These transform records one at a time, keep nothing between records, and
map cleanly onto a Consumer→transform→Producer loop.

| Operation | Purpose |
|---|---|
| `filter` / `filterNot` | keep / drop records matching a predicate |
| `mapValues` | transform value, **key unchanged** (no repartition) |
| `map` | transform key and value (marks stream for repartition) |
| `flatMapValues` / `flatMap` | one record → zero or more records |
| `selectKey` | set a new key (marks stream for repartition) |
| `peek` | side effect per record, passes the record through unchanged |
| `foreach` | terminal side effect (write to an external store, log) |
| `print` | terminal debug sink to stdout or a file |
| `merge` | combine two streams of the same type into one |
| `to` | terminal: write the stream to an output topic |
| `split` | fan a stream into multiple named branches by predicate |

Prefer the value-only variants (`mapValues`, `processValues`,
`flatMapValues`) whenever the key does not change. They keep the stream on
its existing partitions; the key-changing variants (`map`, `selectKey`,
`flatMap`, `process`) flag the stream as needing repartition, which costs an
extra internal topic the moment you follow them with a stateful op.

### Worked example: processing a purchase

A sale event carries a credit-card number, a discount card, an amount, a
department, and an employee id. A first pass is entirely stateless:

- **mask the card number** — `mapValues`, so the raw PAN never reaches any
  downstream topic;
- **record reward points for the purchase** — `mapValues` to derive points,
  `to` a rewards topic;
- **track the sale** — `mapValues` to a pattern-tracking view, `to` a
  patterns topic;
- **persist the masked transaction** — `to` a storage topic.

Add `filter` to keep only high-value purchases before a "big spenders"
sink, and `selectKey` to re-key by whatever the sink needs.

To route cafe sales and electronics sales to different topics, use `split`:

> **Correction to a common slide.** Older material shows
> `KStream.branch(Predicate...)` returning a `KStream[]` array. **That API
> is removed in Kafka 4.0** (deprecated in 2.8, KIP-418). Use `split()`,
> which returns a `BranchedKStream`, and add named branches:
>
> ```java
> Map<String, KStream<String, Sale>> branches = maskedStream
>     .split(Named.as("dept-"))
>     .branch((k, v) -> v.department().equals("cafe"),
>             Branched.as("cafe"))
>     .branch((k, v) -> v.department().equals("electronics"),
>             Branched.as("electronics"))
>     .defaultBranch(Branched.as("other"));
> // branches.get("dept-cafe"), branches.get("dept-electronics"), ...
> ```
>
> Or handle each branch inline with `Branched.withConsumer(ks -> ks.to(...))`.
> Close the split with `defaultBranch(...)` or `noDefaultBranch()`.

Finally, to hand every record for one salesperson to an external database,
`foreach` — a terminal node whose side effect is the external write.

## State

Everything above is stateless: each event is processed on its own. State is
needed the moment an output depends on records seen *earlier*: accumulating
a customer's total reward points to apply a threshold discount, counting
events per key, or joining two streams (a join needs to remember one side
while it waits for the other).

State raises a distribution problem. A Streams application usually runs as
several instances for throughput and failover. Each instance runs one or
more **StreamThreads**, and the work is split into **Tasks** — one Task per
input partition. Each Task owns its **own** state store, mapped to the
corresponding partition of a backing topic. Two consequences follow:

1. **Everything that contributes to one aggregate must live in one
   partition.** If a customer's events are spread across partitions, no
   single Task ever sees the whole customer, and the per-customer total is
   wrong. State is only correct when it is partitioned by the same key the
   state is keyed on.

2. **Repartition when the input key is wrong or absent.** If the input has
   no key (records spread randomly across partitions) or the key is not the
   one you aggregate on, you must move records so co-aggregated records
   share a partition. `repartition()` writes to an internal topic keyed the
   way you need and reads it back; `StreamPartitioner` lets you control the
   partition function when the default hash is not what you want.

```
input (no useful key)            repartition by customerId
+---+---+---+---+                 +----------+----------+
| p0| p1| p2| p3|   --repartition--> | cust A,C | cust B,D |
+---+---+---+---+                 +----------+----------+
 events scattered                 each customer now on one partition
```

Key-changing DSL operations (`selectKey`, `map`, `flatMap`, `process`) set
an internal "needs repartition" flag. When you then perform a **join,
grouping, or aggregation**, Streams inserts the repartition topic
**automatically** — you do not call `repartition()` yourself in that case.
Value-only operations never set the flag.

### Creating a state store

```java
StreamsBuilder builder = new StreamsBuilder();

String storeName = "rewardsPointsStore";
StoreBuilder<KeyValueStore<String, Integer>> storeBuilder =
    Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore(storeName),   // supplier
        Serdes.String(),                              // key serde
        Serdes.Integer());                            // value serde

builder.addStateStore(storeBuilder);

// attach the store to a processor by name:
stream.process(() -> new RewardProcessor(storeName), storeName);
```

Inside the processor, retrieve the store with
`context.getStateStore(storeName)`, read the running total, add the new
points, write it back, and forward the enriched record.

> **Correction to a common slide.** The builder class is **`StreamsBuilder`**
> (plural "Streams"), not `StreamBuilder`. The store methods are on it:
> `builder.addStateStore(...)`.

### Store types

- `Stores.persistentKeyValueStore(name)` — backed by **RocksDB**, spills to
  local disk, survives large state that would not fit in heap. The default
  and correct choice for most stateful apps.
- `Stores.inMemoryKeyValueStore(name)` — heap-only, fastest, bounded by RAM.
- `Stores.lruMap(name, maxEntries)` — in-memory with LRU eviction.

All three are still fault-tolerant when logging is enabled (below): the
local store is disposable because the truth is in the changelog topic.

### Fault tolerance and locality

Two properties make Streams state usable in production:

- **Locality.** The store lives next to the processor (in-process heap or
  local RocksDB), not in a remote database. Reads and writes do not cross
  the network and do not depend on a remote service being up.
- **Recovery.** Each store is backed by a compacted **changelog topic** in
  Kafka. Every write to the store is also written to the changelog. If the
  instance crashes, another instance replays the changelog for that
  partition and rebuilds the store, then resumes. The changelog is named
  `<application.id>-<store-name>-changelog`.

Logging (the changelog) is **on by default**. Control it on the
`StoreBuilder`:

- `withLoggingDisabled()` — no changelog; state is lost on crash. Only for
  state you can rebuild trivially.
- `withLoggingEnabled(Map<String,String> topicConfig)` — tune the changelog
  topic (retention, compaction, segment size). Keys come from `TopicConfig`.

> **Single-node lab trap (recurring).** Streams creates its internal topics
> — changelogs **and** repartition topics — automatically. Their replication
> factor comes from `StreamsConfig.REPLICATION_FACTOR_CONFIG`
> (`"replication.factor"`), whose default is **-1**, meaning "use the
> broker's `default.replication.factor`". On the single-node lab broker,
> creating a topic with RF > 1 fails, so Streams hangs on startup exactly
> the way `__transaction_state` did in the transactions lesson and
> `_schemas` did in the Schema Registry lesson. **Set it explicitly:**
>
> ```java
> props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
> ```

## KStream vs KTable

Two ways to interpret the records in a topic:

- **KStream** — an unbounded **stream of independent events**. Every record
  is a fact that happened. "Purchase X occurred." Nothing is overwritten.
- **KTable** — a **changelog view**: each record is an **upsert** for its
  key. The table holds the latest value per key; a new record for an
  existing key replaces the old one; a `null` value is a delete (tombstone).
  "The current reward balance for customer C is 340."

`builder.stream(topic)` gives a KStream; `builder.table(topic)` gives a
KTable. A `GlobalKTable` (`builder.globalTable`) is a KTable fully
replicated to every instance — useful as a small lookup table joined without
repartitioning.

You convert between them: `stream.groupByKey().aggregate(...)` produces a
KTable; `table.toStream()` produces a KStream of the change events.

## Grouping, windowing, and aggregation

Aggregation is a stateful reduction over records sharing a key:

```java
KGroupedStream<String, Event> grouped = events.groupByKey();
// or .groupBy((k, v) -> newKey, Grouped.with(...)) to re-key (repartitions)
```

Over an **unbounded** grouping, `count()` / `reduce()` / `aggregate()` yield
a KTable keyed by the record key. But an event stream is infinite — usually
you want the aggregate over a **window** of time, not over all history.
Window types:

- **Tumbling** — fixed-size, non-overlapping (`TimeWindows.ofSizeAndGrace`).
  "Count per 1-minute bucket."
- **Hopping** — fixed-size, overlapping by a smaller advance.
- **Sliding** — window defined by the time difference between records.
- **Session** — dynamic: a window stays open while records keep arriving
  within an *inactivity gap*, and closes when the gap elapses with no new
  record. Session length is data-driven, not fixed.

Windowed aggregation is keyed by a `Windowed<K>` (the key plus the window
bounds), so the result KTable is `KTable<Windowed<K>, V>`.

### Session windows (this drives the homework)

The homework — *count events with the same key within a 5-minute session* —
is a session-windowed count:

```java
KTable<Windowed<String>, Long> counts = builder
    .stream("events", Consumed.with(Serdes.String(), Serdes.String()))
    .groupByKey()
    .windowedBy(SessionWindows.ofInactivityGapAndGrace(
            Duration.ofMinutes(5),      // inactivity gap: 5 min
            Duration.ofMinutes(1)))     // grace for late records
    .count(Materialized.as("events-session-counts"));

counts.toStream()
      .foreach((windowedKey, count) ->
          System.out.printf("key=%s window=[%s..%s] count=%d%n",
              windowedKey.key(),
              windowedKey.window().startTime(),
              windowedKey.window().endTime(),
              count));
```

A session for a key opens on its first event, extends every time another
event for that key arrives within 5 minutes of the previous one, and closes
once 5 minutes pass with no new event for that key. Sessions with a gap
larger than 5 minutes are separate windows with separate counts.

> **Correction to a common slide.** Older material builds session windows
> with `SessionWindows.with(Duration)`. That factory is deprecated; use
> **`SessionWindows.ofInactivityGapAndGrace(gap, grace)`** (or
> `ofInactivityGap(gap)`), which makes the grace period explicit. The same
> pattern applies to `TimeWindows.ofSizeAndGrace` and `JoinWindows` below —
> the modern factories all take an explicit grace period.

## Joins

A join combines records from two streams (or a stream and a table) by key.
It resembles a SQL join, but the inputs are unbounded, so a stream-stream
join is always **windowed**: two records join only if their keys match *and*
their timestamps fall within a join window. Joining "over the whole table"
is impossible for an infinite stream.

Three variants:

- `join` — inner: emit only when a matching key exists in **both** streams
  within the window.
- `leftJoin` — every record from the left, plus matches from the right
  (right side may be null).
- `outerJoin` — every record from both sides; matches within the window are
  combined.

The combine logic is a `ValueJoiner`:

```java
interface ValueJoiner<V1, V2, VR> {
    VR apply(V1 value1, V2 value2);
}
```

The window:

```java
JoinWindows window =
    JoinWindows.ofTimeDifferenceAndGrace(
        Duration.ofMinutes(20),   // records within 20 min of each other match
        Duration.ofMinutes(5));   // grace for late arrivals
```

### Worked example: the free-coffee coupon

Issue a coupon to a customer who buys electronics **and** orders in the cafe
within a 20-minute window. Two topics — electronics purchases and cafe
orders — keyed by customer, joined:

```java
electronics.join(
    cafe,
    (electronicsSale, cafeOrder) -> new Coupon(cafeOrder.customerId()),
    JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(20), Duration.ofMinutes(5)),
    StreamJoined.with(Serdes.String(), electronicsSerde, cafeSerde))
  .to("coupons");
```

Because a stream-stream join needs both sides co-partitioned by the join
key, and the raw topics are keyed differently (or not at all), a `selectKey`
before the join sets the join key — and Streams then inserts the repartition
topics automatically, as described in the state section. This is the
"automatic repartitioning" behaviour: any key-changing operation followed by
a join / grouping / aggregation triggers it without an explicit
`repartition()` call.

## Time in a stream

Windows and joins depend on *which* timestamp each record carries. Sources
of time:

1. **From the event** — the record timestamp, which may have been set by the
   producer at send time, by the client library, or stamped by the broker on
   arrival (`message.timestamp.type` = `CreateTime` vs `LogAppendTime`); or a
   timestamp extracted from inside the payload.
2. **From the processing clock** — the wall-clock time at the moment the
   record is processed.

Configure via `TimestampExtractor` (set on `Consumed` or as the default in
`StreamsConfig`):

- **`FailOnInvalidTimestamp`** — the default; uses the record timestamp,
  throws on a negative/invalid one.
- **`LogAndSkipOnInvalidTimestamp`** — logs and drops records with invalid
  timestamps.
- **`UsePreviousTimeOnInvalidTimestamp`** — substitutes the last valid time.
- **`WallclockTimestampExtractor`** — ignores the record timestamp and uses
  processing time. Choose this only when event time is meaningless for the
  use case; it makes windowing non-deterministic on reprocessing.

The first three extend `ExtractRecordMetadataTimestamp` (event time); the
last is processing time. For the session-window homework, event time
(`FailOnInvalidTimestamp`, the default) is correct — the console producer
stamps each record with `CreateTime`, and sessions are measured in event
time.

## Key takeaways

- Kafka Streams is a **library** over the Producer/Consumer, expressing work
  as a **topology** (DAG) of source/stream/sink processors.
- **Stateless** operations (`filter`, `mapValues`, `flatMap`, `selectKey`,
  `split`, `merge`, `peek`, `foreach`, `to`) map to a transform loop. Prefer
  value-only variants to avoid repartitioning.
- **State** lives in local stores (RocksDB / in-memory), partitioned per
  Task, made durable by **changelog topics**. Co-aggregated records must
  share a partition — repartition when the key is absent or wrong.
- **KStream** = event stream; **KTable** = upsert/changelog view. Windowed
  aggregation over a `KGroupedStream` yields a `KTable<Windowed<K>, V>`.
- **Session windows** (`ofInactivityGapAndGrace`) group events per key by
  inactivity gap — the mechanism behind the homework.
- **Joins** on streams are windowed (`ofTimeDifferenceAndGrace`),
  combine via a `ValueJoiner`, and trigger automatic repartitioning after a
  key change.
- **Time** defaults to event time (`FailOnInvalidTimestamp`); windows and
  joins are only as correct as the record timestamps.
- On a **single-node broker**, force `replication.factor=1` in
  `StreamsConfig` or internal-topic creation hangs on startup.

## References

- Kafka Streams Developer Guide — Streams DSL, Processor API, state stores,
  windowing, joins.
- KIP-418 — deprecation of `KStream.branch` (removed in 4.0; use `split`).
- KIP-820 — deprecation of `Transformer`/`ValueTransformer` (removed in 4.0;
  use `process`/`processValues` with the `processor.api` package).
- `org.apache.kafka.streams.kstream` — DSL types.
- `org.apache.kafka.streams.processor.api` — Processor API.
- `org.apache.kafka.streams.state.Stores` — store suppliers and builders.
- `org.apache.kafka.streams.StreamsConfig` — `REPLICATION_FACTOR_CONFIG` and
  defaults.
