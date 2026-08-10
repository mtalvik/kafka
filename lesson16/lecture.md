# Lesson 16 — Kafka Streams: transactions, Processor API, Interactive Queries

Lesson 15 covered the DSL: `filter`, `mapValues`, `groupByKey().count()`,
windows, joins. That surface is declarative and covers most work, but it
leaves three things unaddressed.

First, **correctness under failure**. A Streams application reads records,
updates local state, writes output, and commits offsets — four separate
effects that must either all happen or none of them. Lesson 10 introduced
transactions at the Producer level; here they become a single configuration
flag, with consequences worth understanding.

Second, **the layer under the DSL**. `StreamsBuilder.build()` compiles down
to a `Topology` of processor nodes. You can build that `Topology` by hand,
and there are operations — timers, conditional forwarding, direct store
access — that the DSL cannot express.

Third, **reading the state back out**. So far, results only leave the
application through an output topic. Interactive Queries let a service read
its own state stores directly, without a database in the middle.

## Part 1 — Transactions in Kafka Streams

### What is being made atomic

Recall the Producer-level machinery from lesson 10. The **idempotent
producer** (on by default: `acks=all`, `retries>0`,
`max.in.flight.requests.per.connection<=5`) deduplicates retries using a
producer ID and per-partition sequence numbers, and preserves ordering
across those retries. **Transactions** build on top: a producer writes
records *and* consumer offsets inside a transaction, then commits or aborts
the whole set, coordinated by a `TransactionCoordinator`.

A Streams task performs three writes per record batch:

1. output records to downstream topics,
2. changelog records for any state store it updated,
3. the consumer offsets for the input partitions it read.

With transactions enabled, all three go into one transaction. Either the
whole batch is visible and the offsets advance, or none of it is and the
batch is reprocessed after restart. That is what "exactly-once" means here:
not that a record is physically delivered once, but that its *effects* are
applied once.

```
              +---------------------------+
  input  ---> |  kafka streams (one txn)  | ---> output topic
              |  process + update state   | ---> __consumer_offsets
              +---------------------------+ ---> changelog topic
                   all committed, or all aborted
```

### Turning it on

One config:

```java
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
```

That is the whole application-side change. Streams sets the underlying
producer's `transactional.id`, enables idempotence, and switches the internal
consumer to `isolation.level=read_committed` for repartition and changelog
reads.

> **Only v2 exists.** `exactly_once` (v1) and `exactly_once_beta` were
> deprecated in 3.0 and **removed in Kafka 4.0**. Material that offers a
> choice between v1 and v2 predates 4.x. Requires broker 2.5+.

### v1 vs v2, and why the difference shows up in latency

The distinction matters because a common slide gets the consequence wrong.

- **v1** used **one producer per Task**. With N tasks on a thread, N
  transactional producers, N transactions per commit.
- **v2** uses **one producer per `StreamThread`**. The thread opens a single
  transaction covering all of its tasks and commits once. Fewer producers,
  fewer transaction markers, less coordinator load — the reason v1 was
  dropped.

> **Correction.** Older slides state "4 subtopologies = 4 transactions = 400 ms
> of added latency". The *count* is v1 reasoning and no longer holds — under
> v2 a thread commits once, not once per task.
>
> The *latency* conclusion survives, for a different reason. A record
> crossing a subtopology boundary is written to a repartition topic, and the
> next subtopology reads it with `read_committed` — so it cannot see the
> record until the upstream commit. Each hop therefore waits up to one
> commit interval. Four subtopologies ≈ 4 × `commit.interval.ms` of added
> end-to-end latency. Say "latency stacks per subtopology boundary", not
> "N transactions".

### Commit interval

`commit.interval.ms` defaults to **30000 ms** normally, and to **100 ms**
when a processing guarantee of `exactly_once_v2` is set — Streams lowers it
because under transactions the commit interval *is* the latency floor.

Throughput is barely affected: transactions batch, and the marker writes are
small. Latency is what you pay. Tune deliberately:

- lower `commit.interval.ms` → lower latency, more transaction markers and
  more coordinator traffic;
- raise it → more efficient, but every downstream `read_committed` reader
  waits longer.

### Downstream consumers

A plain consumer has `isolation.level=read_uncommitted` and sees records
from aborted transactions. Consumers that must not see them set
`read_committed`, which has two consequences worth stating to students:

- an **open** transaction blocks the reader — it cannot advance past the
  last stable offset, so a stalled producer stalls the consumer;
- an abandoned transaction is aborted by the coordinator on timeout, after
  which the reader proceeds.

> **Timeout figure.** Slides commonly quote 60 s. That is the broker/producer
> default for `transaction.timeout.ms`, but Streams overrides it for its own
> producer: the effective value is **10 s**, confirmed against Kafka 4.3 via
> `kafka-transactions.sh describe` (`TransactionTimeoutMs 10000`). So an
> abandoned transaction blocks `read_committed` readers for ten seconds, not a
> minute.

### State stores and crash recovery

Local stores are **not** transactional with the rest of the batch. On a
crash mid-transaction the output and offsets are rolled back, but the local
RocksDB directory may already contain the uncommitted updates. Streams
handles this by discarding the local store on restart and **replaying the
changelog topic** from the last committed offset. Correct, but the restore
can be slow for large stores.

KIP-892 ("Transactional Semantics for StateStores") addresses exactly this
by writing store updates into a transactional buffer so the local store can
be rolled back instead of rebuilt. Check its status against the running
broker/client version before describing it as future work.

### Cluster-side prerequisites

Transactions add requirements beyond the application config:

- `__transaction_state` is created with
  `transaction.state.log.replication.factor` (default 3) and
  `transaction.state.log.min.isr` (default 2). On a **single-node broker
  both must be 1**, or transaction initialisation hangs and then fails.
- Internal Streams topics — changelog and repartition — are created with
  `StreamsConfig.REPLICATION_FACTOR_CONFIG`, default `-1` (broker default,
  typically 3). **Set it to 1** on a single node. This is the same failure
  class as lessons 14–15, now with a third variant.
- With ACLs enabled, the principal needs new grants: `Write` and `Describe`
  on a `TransactionalID` resource matching the `transactional.id` Streams
  derives from `application.id`, plus `IdempotentWrite` on the `Cluster`.
  Because the transactional ID is derived from `application.id`, a stable
  `application.id` lets one prefixed ACL cover the application — a
  randomised one (`"app-" + UUID.randomUUID()`) leaves every run
  unauthorised.

## Part 2 — Processor API

### Building a topology by hand

`StreamsBuilder` is a builder *for* a `Topology`. The Processor API skips it
and constructs the graph directly, naming every node and wiring parents
explicitly:

```java
Topology topology = new Topology();
topology
    .addSource("source", keyDeserializer, valueDeserializer, "src-topic")
    .addProcessor("upper", CaseProcessor::new, "source")
    .addProcessor("logger", LogProcessor::new, "upper")
    .addSink("sink", "out-topic", keySerializer, valueSerializer, "upper");
```

The last argument of each call is the list of **parent node names**. Here
`upper` fans out to two children, `logger` and `sink` — the DAG shape from
lesson 15, written out rather than inferred.

Four node kinds: `addSource`, `addProcessor`, `addSink`, and
`addStateStore` (attached to named processors). `topology.describe()` prints
the resulting graph and, importantly, the **subtopology** split — the same
output the DSL produces, which is how you count the transaction hops from
Part 1.

### The Processor interface

```java
static class CaseProcessor implements Processor<String, String, String, String> {
    private ProcessorContext<String, String> context;

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, String> record) {
        context.forward(new Record<>(record.key(),
                                     record.value().toUpperCase(),
                                     record.timestamp()));
    }
}
```

Type parameters are `<KIn, VIn, KOut, VOut>`. Three lifecycle methods:
`init` (called once per task, stash the context and look up stores),
`process` (once per record), `close` (release resources — note that
Streams closes the stores itself).

`ProcessorContext` is the handle to everything around the record:

- `forward(Record)` — send to all children;
  `forward(Record, String childName)` — send to one named child. There is no
  DSL equivalent of selective forwarding, which is one of the reasons to
  drop to this level.
- `getStateStore(name)` — the store declared via `addStateStore` or
  connected through the DSL.
- `schedule(...)` — timers, below.
- `recordMetadata()` — an `Optional<RecordMetadata>` with topic, partition
  and offset of the record being processed. Empty when the record originated
  from a punctuator rather than an input topic.
- `taskId()`, `applicationId()`, `commit()` (a *request* to commit, not an
  immediate commit).

A processor that forwards nothing is a terminal node — the logger above is
exactly that.

### FixedKeyProcessor

`FixedKeyProcessor<KIn, VIn, VOut>` is the value-only variant: it receives a
`FixedKeyRecord`, and can only `record.withValue(...)`. Because the key
provably does not change, Streams knows no repartitioning is needed
downstream. Prefer it whenever the key is untouched — the same reasoning as
`mapValues` over `map` in lesson 15.

> **Removed API.** `Transformer` / `ValueTransformer` with
> `transform()` / `transformValues()` were deprecated in 3.3 (KIP-820) and
> **removed in 4.0**. Any example using them will not compile. The
> replacements are `Processor` / `FixedKeyProcessor` in
> `org.apache.kafka.streams.processor.api`, reached via `process()` /
> `processValues()`.

### Punctuation — running code on a timer

A processor normally only runs when a record arrives. `schedule` adds a
periodic callback:

```java
Cancellable c = context.schedule(
        Duration.ofSeconds(10),
        PunctuationType.WALL_CLOCK_TIME,
        timestamp -> { /* runs every 10 seconds */ });
```

> **Naming.** The interface is **`Punctuator`**, a functional interface with
> `void punctuate(long timestamp)`. Slides that write `Punctuate.punctuate()`
> have the type name wrong.

Two clock types, and the choice is the entire lesson:

- **`STREAM_TIME`** — driven by the timestamps of records flowing through the
  task. It advances only when records arrive. **If the input goes quiet, the
  punctuator never fires.** Same trap as `suppress(untilWindowCloses)` in
  lesson 15.
- **`WALL_CLOCK_TIME`** — driven by the system clock, fires regardless of
  traffic. Non-deterministic on reprocessing, but the right choice for
  "emit a report every 10 seconds" or "expire idle entries".

Both are best-effort: the callback fires during the poll loop, so a long
`process()` delays it. `schedule` returns a `Cancellable` — cancel it if the
schedule is per-key and the key is done, or the schedules accumulate.

The canonical use case: a stateful processor updates a store on every
record, and a wall-clock punctuator sweeps the store every N seconds and
forwards a batch of results. Per-record write, periodic read — a shape the
DSL cannot express.

### Mixing with the DSL

The Processor API does not have to replace the DSL. Two DSL operators drop
into it mid-chain:

```java
stream.process(MyProcessor::new, "my-store");        // may change the key
stream.processValues(MyFixedKeyProcessor::new, "my-store");  // key preserved
```

The trailing arguments name state stores to connect to the processor. This
is the normal way to use the Processor API in production: DSL for the
plumbing, a processor node where the DSL runs out.

> **Correction to a slide takeaway.** "There is no point using the Processor
> API any more" overstates it. The DSL is the default, but the Processor API
> remains the only way to: schedule punctuation, forward selectively to named
> children, read *and* write a store with arbitrary logic, implement
> dead-letter routing, or inspect record metadata during processing. What is
> obsolete is the *`Transformer`* API, not the Processor API.

## Part 3 — Interactive Queries

### The problem

A Streams application that counts events per key holds the answer in a local
store, but the only way out so far has been the output topic. The usual
architecture bolts a second service onto that topic to write into a
database, which the REST layer then queries:

```
Kafka -> Streams -> output topic -> sink service -> database <- REST API
```

Three moving parts to keep the current count available. Interactive Queries
collapse them: the Streams application exposes its own store, and the REST
layer reads it in-process.

```
Kafka -> Streams (state store) <- REST API
```

The store is materialised the usual way — nothing special is required of the
topology:

```java
builder.stream("events", Consumed.with(stringSerde, stringSerde))
       .groupByKey()
       .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count-store")
                          .withKeySerde(stringSerde)
                          .withValueSerde(longSerde));
```

A named store is a queryable store.

### Reading the local store

```java
ReadOnlyKeyValueStore<String, Long> store = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType("count-store",
                                             QueryableStoreTypes.keyValueStore()));
Long value = store.get(key);
```

The type is **read-only** by design: writes must go through the topology,
or the changelog and the store diverge.

Two failure modes to handle rather than discover in a demo:

- calling `store(...)` before the client reaches `RUNNING` throws
  `StreamsNotStartedException` / `InvalidStateStoreException`. Retry, or
  register a `StateListener` and only expose the endpoint once running.
  During a rebalance the store becomes unavailable again.
- `store.get(key)` returns `null` for a key that has not been seen. That is
  not an error — it is a 404, and must not reach the caller as a
  `NullPointerException`.

### One store per task — the distributed part

A store is partitioned exactly like the input topic: **one store instance
per task**, and tasks are spread across application instances. An instance
holds only the keys whose partitions it currently owns. Asking instance A
for a key owned by instance B gets a local answer of `null`, which is wrong,
not merely empty.

So each instance must be able to (a) work out which instance owns a key and
(b) reach it.

**(a) Ownership.** Every instance advertises where it can be reached:

```java
props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, host + ":" + port);
```

Streams gossips that string through the consumer group protocol, so every
instance knows the full membership map. Then:

```java
KeyQueryMetadata metadata =
        kafkaStreams.queryMetadataForKey("count-store", key, stringSerde.serializer());
HostInfo active = metadata.activeHost();
```

Streams hashes the key with the given serializer, determines the partition,
and returns the host owning it — plus `standbyHosts()` for any standby
replicas.

> **Deprecated method.** `allMetadataForKey` returned only `StreamsMetadata`
> and was superseded by `queryMetadataForKey` in 2.5 (KIP-535), which returns
> `KeyQueryMetadata` with the active/standby distinction. Slides still
> showing `allMetadataForKey` are pre-2.5.

When the assignment is not yet known — during a rebalance — the result is
`KeyQueryMetadata.NOT_AVAILABLE`, whose host is the sentinel
`HostInfo("unavailable", -1)`. Check for it; do not dereference it as a real
address.

**(b) Reaching it.** **Kafka Streams provides no RPC mechanism.** It tells
you the `host:port` and stops there. The transport is yours — HTTP, gRPC,
whatever the service already speaks. The routing logic is always the same
shape:

```java
HostInfo host = metadata.activeHost();
if (isSelf(host)) {
    return localStore.get(key);      // answer locally
}
return httpGet(host, key);           // forward to the owning instance
```

`isSelf` compares against this instance's own `APPLICATION_SERVER_CONFIG`
value. When several instances run on one machine, comparing the port is
enough; across machines the host must be compared too.

### Behaviour under rebalance

Start a second instance and partitions are reassigned: keys that were local
become remote, and queries start being forwarded. Stop it and, after the
rebalance, its partitions come back — but the store must first be **restored
from the changelog topic** before it can answer. The window between
assignment and restoration is exactly when `InvalidStateStoreException`
appears. `num.standby.replicas` shortens it by keeping warm copies on other
instances; `standbyHosts()` is what lets a query be served (possibly
slightly stale) from one of them.

### IQv2

KIP-796 (3.2) added a second query interface,
`KafkaStreams.query(StateQueryRequest)`, extensible to query types beyond
key lookup and range scan. The classic API above is not deprecated and
remains the straightforward one to learn; IQv2 is worth knowing exists.

## Key takeaways

- **`processing.guarantee=exactly_once_v2`** makes output records, changelog
  writes, and offset commits one atomic unit. Only v2 exists in 4.x.
- v2 uses **one producer per StreamThread**, not per task. "N subtopologies
  = N transactions" is v1 reasoning; what actually stacks is **latency**, one
  commit interval per subtopology boundary, because of `read_committed` on
  repartition topics.
- `commit.interval.ms` drops to **100 ms** under EOS. It is the latency
  floor; throughput is barely affected.
- Local stores are not transactional — on crash they are **rebuilt from the
  changelog**.
- Transactions on a single-node broker need
  `transaction.state.log.replication.factor=1`, `min.isr=1`, and
  `StreamsConfig.REPLICATION_FACTOR_CONFIG=1`; with ACLs, `Write`/`Describe`
  on `TransactionalID` and `IdempotentWrite` on `Cluster`.
- **Processor API**: `addSource` / `addProcessor` / `addSink` /
  `addStateStore`, wired by parent name. `Processor` (key may change) and
  `FixedKeyProcessor` (key preserved, no repartition).
- **`Punctuator`** via `context.schedule(Duration, PunctuationType, ...)`.
  `STREAM_TIME` advances only with incoming records; `WALL_CLOCK_TIME` fires
  regardless. Cancel per-key schedules via the returned `Cancellable`.
- `process()` / `processValues()` embed a processor inside a DSL chain — the
  normal production shape. `Transformer` is removed, the Processor API is
  not.
- **Interactive Queries** read a named store in-process, removing the sink
  service and the database. Stores are per-task, so
  `APPLICATION_SERVER_CONFIG` + `queryMetadataForKey` route a key to its
  owning instance — and **you write the RPC yourself**.
- Handle `InvalidStateStoreException` (starting or rebalancing), `null`
  values (unknown key), and the `"unavailable"` sentinel host.

## References

- Bill Bejeck, *Kafka Streams in Action* (Manning) — the book the OTUS deck
  cites; the stock-performance example in Ex3 comes from it.
- Kafka Streams Developer Guide — Processor API, Interactive Queries,
  processing guarantees.
- Punctuate use cases:
  `https://cwiki.apache.org/confluence/display/KAFKA/Punctuate+Use+Cases`
- KIP-447 — producer-per-thread scalability for exactly-once (`exactly_once_v2`).
- KIP-535 — `queryMetadataForKey`, querying standby replicas.
- KIP-796 — Interactive Query v2.
- KIP-820 — removal of `Transformer`/`ValueTransformer`.
- KIP-892 — transactional semantics for state stores.
- `org.apache.kafka.streams.processor.api` — `Processor`, `FixedKeyProcessor`,
  `ProcessorContext`, `Record`.
- `org.apache.kafka.streams.processor` — `Punctuator`, `PunctuationType`,
  `Cancellable`.
- `org.apache.kafka.streams.state` — `QueryableStoreTypes`, `HostInfo`,
  `ReadOnlyKeyValueStore`.
