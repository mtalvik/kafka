# Kafka Streams DSL: KStream, KTable, GlobalKTable

## 1. Scope

This lesson covers the high-level **DSL** of Kafka Streams: the two core abstractions (`KStream`, `KTable`), their global variant (`GlobalKTable`), and the operations built on them — stateless transforms, aggregations, windowing, and joins. The Processor API (low-level topology, custom state stores, punctuators) is a separate lesson.

The lab runs against the `kafka` EC2 broker (KRaft, single node, SASL/PLAIN), not a local Docker stack. That changes two things versus the reference material: the app needs SASL client config, and every internal topic Streams creates inherits the broker's replication factor — see section 8.

## 2. Stream/table duality

A topic can be read two ways, and the choice is semantic, not cosmetic.

- **`KStream`** — each record is an independent fact. Two records with the same key are two events, both kept. Semantics: **INSERT**. Reading a click stream, a transaction log, a sensor feed.
- **`KTable`** — each record is the *current value* for its key. A later record with the same key **replaces** the earlier one. Semantics: **UPSERT** (and a `null` value is a **DELETE**, a tombstone). A `KTable` is a materialized view of the latest-value-per-key, backed by a local state store.

The two are duals. A table is a snapshot of a stream folded by key; a stream is the changelog of a table. Given the changelog `("alice",1) ("charlie",1) ("alice",2) ("bob",1)`, the table evolves `{alice:1} → {alice:1,charlie:1} → {alice:2,charlie:1} → {alice:2,charlie:1,bob:1}`. Replaying the changelog reconstructs the table exactly — this is why a materialized `KTable` can be rebuilt from its changelog topic after a crash.

**Table as a compacted topic.** The mental model "a KTable is a compacted topic" is useful: log compaction keeps only the latest value per key, which is exactly the table view. Two caveats worth stating to students:

> **Correction to a common slide.** Materializing a `KTable` from a source topic does **not** require that topic to have `cleanup.policy=compact`. You can build a `KTable` over a normal retention topic; Streams folds by key in the state store regardless. Compaction is the *analogy*, and it is a real guarantee only for the internal **changelog** topic that Streams creates for a materialized table — that one is compacted so the store can be restored.

## 3. KTable caching and emit rate

A `KTable` does not necessarily emit a downstream record for every input record. Updates pass through a record cache first; only some of them propagate.

- More input throughput → more frequent emits.
- More distinct keys → more frequent emits (less dedup benefit).
- Emits also happen on commit, and when the cache fills and flushes to the state store.

The cache deduplicates: if key `YERB` is updated twice between flushes, only the latest value is forwarded — the intermediate value is never stored and never emitted. This is a throughput optimization, not a correctness guarantee, and it means **you cannot rely on seeing every intermediate update downstream** from a KTable.

> **Correction to a common slide.** `cache.max.bytes.buffering` is **deprecated** (KIP-770, Kafka 3.4) in favour of `statestore.cache.max.bytes`. On the 4.x client the old name still resolves but logs a deprecation warning; use the new one. To see every update in a demo, set the cache to `0` — do not present the old config name as current.

`commit.interval.ms` controls flush/commit frequency. Default is `30000` ms, but it drops to `100` ms automatically when `processing.guarantee=exactly_once_v2`. That interacts with caching: with EOS on, emits are far more frequent even at the same cache size.

## 4. Aggregations

`groupByKey()` (key unchanged) or `groupBy()` (new key, forces repartition) turns a `KStream` into a `KGroupedStream`, on which you aggregate:

- **`count()`** — number of records per key.
- **`reduce(v1, v2 -> v)`** — combine two values of the **same** type. No type change.
- **`aggregate(initializer, (k, v, agg) -> agg')`** — general fold; the aggregate can be a **different** type than the input (an `Initializer` provides the zero). Use this when counting into a struct, summing into an object, etc.

All three return a `KTable` — the running aggregate is a latest-value-per-key view.

Grouping a **table** gives a `KGroupedTable` with a similar interface, minus `windowedBy`. One asymmetry that the slides skip and that trips people up:

> **Correction to a common slide.** `KGroupedTable.aggregate` and `reduce` take **two** functions, an **adder and a subtractor**. Because a table update replaces a previous value, the aggregate must first *subtract* the old contribution of that key and then *add* the new one. A `KGroupedStream` has no subtractor — stream records are inserts, nothing to undo. Presenting table aggregation with a single function produces wrong results on updates.

`groupBy` that changes the key writes to an internal **repartition** topic and reads it back, so the new key lands on the correct partition. This is automatic but not free — it is real network and disk I/O.

## 5. Windowing

Non-windowed aggregation runs forever over all history. Windowing bounds it in event time: "operations per ticker per 10 minutes", "clicks per banner per 20 minutes", updated continuously. Kafka Streams has four window types.

| Window | API | Overlap | Defined by |
|---|---|---|---|
| Tumbling | `TimeWindows.ofSizeAndGrace(size)` | none | fixed size, advance = size |
| Hopping | `TimeWindows.ofSizeAndGrace(size).advanceBy(hop)` | yes, if hop < size | fixed size + advance interval |
| Sliding | `SlidingWindows.ofTimeDifferenceAndGrace(diff)` | yes | max time-difference between records |
| Session | `SessionWindows.ofInactivityGapAndGrace(gap)` | data-driven | inactivity gap between records |

> **Correction to a common slide.** The reference deck labels a "20-second window that advances every 5 seconds" as a **sliding** window. In the Kafka Streams DSL that is a **hopping** window (`TimeWindows...advanceBy`). A real `SlidingWindows` (KIP-450, since 2.7) is not a fixed hop — it is defined by the maximum time *difference* between two records and is aligned to record timestamps, producing a window boundary around each event. A student who reaches for `SlidingWindows.ofTimeDifference...` expecting the 5-second hop gets different semantics. Keep the four names distinct: tumbling = advance equals size; hopping = fixed advance; sliding = time-difference; session = inactivity gap.

Two things the deck omits that belong in the lecture:

- **Grace period.** Every windowed operation takes a grace period (`...AndGrace(Duration)`). Late records within grace still update their window; past grace they are dropped. There is no such thing as a windowed aggregation without a grace decision — the API forces you to make it explicit.
- **Suppression.** By default a windowed KTable emits intermediate results as the window fills. `suppress(Suppressed.untilWindowCloses(...))` holds output until the window closes plus grace, so downstream sees one final result per window instead of a stream of partials. This is how you get "one number per 10-minute bucket".

A windowed aggregation is keyed by a `Windowed<K>` (original key + window bounds), and its changelog/store has a retention tied to window size + grace.

## 6. Joins

The hard prerequisite for `KStream`/`KTable` joins is **co-partitioning**: both sides keyed the same way, same number of partitions, same partitioning strategy. If the keys don't line up on the same partition, matching records never meet. Streams repartitions automatically when it can detect a key change; it cannot fix a partition-count mismatch — that is on you.

| Join | Inner | Left | Outer | Windowed |
|---|---|---|---|---|
| KStream–KStream | yes | yes | yes | yes (required) |
| KStream–KTable | yes | yes | — | no |
| KTable–KTable | yes | yes | yes | no |
| KStream–GlobalKTable | yes | yes | — | no |

The matrix is worth memorizing: **only stream–stream and table–table support outer**; stream–table and stream–global are lookups, so outer makes no sense (there is nothing to emit for a right-side row with no stream event).

- **KStream–KStream** is windowed — two events "join" only if they fall within the join window of each other. Needs a `JoinWindows`.
- **KStream–KTable** is a lookup: each stream event is enriched with the current table value for its key. No window.
- **KStream–GlobalKTable** — see next section; the key differentiator is no co-partitioning and a `KeyValueMapper` to pick the lookup key.

## 7. GlobalKTable vs KTable

A `KTable` is **partitioned**: each task holds only the shard of the table for the partitions it owns. That is why joins against a KTable require co-partitioning — the matching key must be in the same task.

A `GlobalKTable` is **fully replicated**: every application instance loads **all** partitions of the topic into a local store at startup. Consequences:

- **No co-partitioning for joins.** A `KStream–GlobalKTable` join uses a `KeyValueMapper` that extracts the lookup key from the *stream* record, then looks it up in the full local copy. The stream does **not** need to be re-keyed or repartitioned to match — this is the whole point.
- **Read-only.** A GlobalKTable supports joins and manual lookups (via a store) and nothing else — no aggregations, no further stateful transforms.
- **Cost.** Every instance holds the entire table and must bootstrap it before processing. Fine for small, slowly-changing reference data (currency codes, company names, feature flags); wrong for large or high-churn data.

The classic use: an enrichment join where re-keying the main stream just to co-partition it would trigger several repartition round-trips. `selectKey` before a KTable join writes to a repartition topic and reads it back each time; with reference data small enough to replicate, a GlobalKTable removes all of that.

## 8. Internal topics and the single-node trap

Streams silently creates internal topics: **repartition** topics (for key-changing operations) and **changelog** topics (to back materialized stores), all prefixed with `application.id`.

> **Correction to a common slide / lab trap.** These internal topics are created with `StreamsConfig.REPLICATION_FACTOR_CONFIG` (`replication.factor`), whose default is `-1` = "use the broker default". On the single-node lab broker, if the broker's `default.replication.factor` is greater than 1, **the application fails at startup creating its internal topics.** Set `replication.factor=1` in the Streams config explicitly. This is the same failure class as `__transaction_state` (transactions lesson) and `_schemas` (Schema Registry lesson): any Kafka-managed topic inherits a replication factor the single broker cannot satisfy.

Related config to know: `application.id` (doubles as the consumer group id and the internal-topic prefix), `num.stream.threads` (parallelism within an instance; total tasks ≤ partition count), and `default.key/value.serde`.

## 9. Running on our infrastructure

The topology config is a `Properties`/`StreamsConfig` object. Against the `kafka` EC2 broker it must carry SASL:

- `bootstrap.servers` → broker private IP:9092
- `security.protocol=SASL_PLAINTEXT`, `sasl.mechanism=PLAIN`
- `sasl.jaas.config` → `PlainLoginModule` with a streams principal
- `replication.factor=1` (section 8)
- `application.id` unique per exercise (each gets its own group + internal-topic namespace)

ACLs for the streams principal: **read** on source topics, **write** on sink topics, **read+write** on the internal repartition/changelog topics (prefix = `application.id`), and group access for the consumer group. Manage these through the GitOps Terraform, not by hand.

Build with Gradle on the `kafka` EC2 (t3.small). The first `gradle build` pulls the world and burns CPU credits; expect it to be slow once and fast after. Exercises are named `ex1`–`exN` as Gradle tasks.
