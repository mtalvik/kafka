# Stream Processing and the Dataflow Model

## 1. Scope

This lesson is conceptual, not a broker lab. The goal is to build the vocabulary that every stream-processing engine — Kafka Streams included — is built on: unbounded data, event time vs processing time, windows, watermarks, triggers, and how partial results are refined. Apache Beam is used as the reference API because it exposes these concepts directly and cleanly; the concepts transfer to Kafka Streams, Flink, and Spark Structured Streaming with different names attached.

## 2. Bounded vs unbounded, not "batch vs streaming"

A **data stream** (event stream) is an abstract representation of an *unbounded* dataset: potentially infinite in size, continuously growing.

The useful distinction is **bounded vs unbounded** data, not batch vs streaming. Batch and streaming are properties of an *execution engine*, not of the data. Unbounded data has been processed by repeated batch runs for decades, and a well-designed streaming engine processes bounded data without trouble. Keep "batch" and "streaming" for runtimes; use "bounded/unbounded" for data.

## 3. Properties of a stream

- **Immutable / append-only.** A stream is an append-only log. Once written, an event is not modified; it is the source of truth.
- **Replayable — with a caveat.** A stream can be re-read from an earlier point to rebuild state or derive new views, *as long as the data is still retained*. Replayability is a function of retention, not an inherent guarantee.
- **Unbounded.** Potentially infinite, continuously growing — which is exactly why windowing exists.
- **Continuous / low-latency.** Processed as records arrive, not on a schedule.

> **Correction to a common slide.** "Ordered (strict order guaranteeing consistency)" is *not* an intrinsic property of a stream. Ordering holds only *within a single partition / per key*. Across a stream, events are generally **out-of-order in event time** — this is the central premise of the Dataflow paper ("unbounded, unordered"). If a stream were globally ordered by event time, watermarks and triggers would be unnecessary. They exist precisely because it is not.

> **Correction to a common slide.** "exactly-once" does not belong on a list of *inherent* stream properties. It is a hard-won delivery guarantee that many systems historically lacked (Storm, Samza, Pulsar shipped without it). It is a property of the *processing pipeline*, achieved through effort, not something a stream has by nature.

## 4. Processing paradigms

| Paradigm | Key property | Latency | Typical use (2026) |
|---|---|---|---|
| Request / Response | synchronous, predictable P99, non-blocking I/O | < 100 ms | online transactions, UIs, model inference |
| Batch | throughput, determinism | seconds–minutes | ETL, reporting, ML training |
| Streaming | continuity, state, exactly-once | seconds and below | real-time analytics, anomaly detection, EDA |
| Unified / Hybrid | one engine, materialized views | near-continuous | Kappa-style platforms, interactive analytics |

The "exactly-once" in the streaming row is a *target the engine works toward*, not a freebie — see the correction above.

**How to read the table.** The paradigms differ along two axes: the *unit of work* (how much is processed at once) and *who waits*.

- **Request / Response** — one request, one immediate answer; the caller blocks until it returns. A REST call or a DB query. Unit of work: a single request.
- **Batch** — collect a large *bounded* pile of data and process all of it at once, on a schedule; nobody waits live. A nightly ETL job. Unit of work: a whole dataset.
- **Streaming** — process each event *as it arrives*, continuously, keeping state across events; the job never "finishes". Fraud scoring on a live click stream. Unit of work: a single event.
- **Unified / Hybrid** — one engine keeps a continuously-updated result that can also be queried like a table; the batch/stream boundary disappears. Unit of work: a materialized view kept fresh.

## 5. Two time domains

Every event carries two clocks:

- **Event time** — when the event actually occurred, at the source.
- **Processing time** — when the pipeline observed it.

Event time for an event essentially never changes. Processing time marches forward constantly. The gap between them is **skew** (processing-time lag / event-time skew), and it is dynamic: it grows when the pipeline lags and shrinks when it catches up. No clock synchronization is assumed across a distributed system. Robust analysis is done in *event time*; processing time is only "when we happened to see it."

## 6. The Dataflow model: four questions

The model decomposes any pipeline into four independent dimensions:

- **What** results are computed → transformations (`ParDo`, `GroupByKey`, combiners).
- **Where** in event time → windowing.
- **When** in processing time results are materialized → watermarks and triggers.
- **How** earlier results relate to later refinements → accumulation mode.

The power of the model is that these four are orthogonal: change *when* you emit without touching *what* you compute; change *how* you refine without changing *where* you window.

## 7. Windowing (the "Where")

Windowing slices an unbounded dataset into finite chunks so that grouping operations (aggregation, joins) can terminate.

- **Fixed / tumbling** — static size; frequency equals length. Daily, hourly.
- **Sliding** — fixed size and period, with period < size, so windows overlap. Fixed windows are the special case where period = size.
- **Sessions** — dynamic, per key, bounded by an inactivity gap. Events closer together than the gap merge into one session.

Windows are also **aligned** (applied across all data for that time range) or **unaligned** (applied to a subset, e.g. per key). Sessions are inherently unaligned. Native support for unaligned event-time windows is the main contribution of the Dataflow model over earlier systems.

## 8. Watermarks (part of the "When")

A **watermark** is a monotonically advancing, usually *heuristic* estimate of event-time progress: a lower bound asserting "we believe all events with event time ≤ W have now been observed."

> **Correction to a common slide.** A watermark is **not** "the timestamp of the oldest event we haven't processed yet." It is a *lower bound on event-time completeness*, and it is normally heuristic. Critically, **late data can and does arrive behind the watermark** — the pipeline advanced W past a point in event time before a straggler for that point showed up. The watermark is an estimate of progress, not a guarantee of completeness. (The slide's own diagram says "monotonically advancing estimate of event-time progress" — the bullet contradicts the diagram.)

Watermarks have two failure modes, which is why they are never used alone:

- **Too fast** — late data arrives behind them, so relying on them solely breaks 100% correctness.
- **Too slow** — a single straggler holds back a global metric, inflating latency for everything.

## 9. Triggers and refinement (the rest of "When" + "How")

A **trigger** decides *when* a window's current contents are emitted as a *pane*. Common trigger conditions:

- **On every element** — `Repeatedly.forever(AfterPane.elementCountAtLeast(1))`.
- **At processing-time intervals** — periodic firing.
- **When the watermark passes the end of the window** — the default, "we think the window is complete."

> **Correction to a common slide.** Per-element firing is **not** "ParDo." `ParDo` is a *transformation* (the "What"), not a trigger. There is no trigger called ParDo. Per-element output in Beam is `Repeatedly.forever(AfterPane.elementCountAtLeast(1))`. Mixing the two conflates the *What* and *When* axes.

**Refinement mode** (the "How") decides how successive panes for the same window relate:

- **Discarding** — window contents dropped after firing; each pane is independent (deltas).
- **Accumulating** — contents kept; each pane refines the previous (overwrite-on-write sinks).
- **Accumulating & retracting** — like accumulating, but a *retraction* of the previous value is emitted before the new one. Required when a downstream `GroupByKey` could otherwise double-count refined results on different keys.

## 10. Worked example (Beam API)

A daily per-key sum, emitting an on-time result at end of day and a refined result if late data arrives within an hour:

```java
PCollection<KV<String, Long>> dailyReport = transactions
    .apply(Window.<KV<String, Long>>into(FixedWindows.of(Duration.standardDays(1)))
        .triggering(
            AfterWatermark.pastEndOfWindow()
                .withLateFirings(AfterPane.elementCountAtLeast(1)))  // refire on late data
        .withAllowedLateness(Duration.standardHours(1))
        .accumulatingFiredPanes())
    .apply(Sum.longsPerKey());
```

*"A report is produced at 00:00; late data arrives at 00:30 — what happens?"* With `.withLateFirings(...)` present and `allowedLateness = 1h`, a second, refined pane fires containing the first report's data plus the late records. **Accumulating** mode is what makes the second pane a superset rather than a bare delta.

> **Correction to a common slide.** The refined 00:30 report is **not** automatic from `AfterWatermark.pastEndOfWindow()` alone. That trigger fires *once* at the watermark by default. Without an explicit `.withLateFirings(...)`, the late data is buffered (within `allowedLateness`) but no second pane is emitted. The late firing must be declared.

> **Correction to a common slide.** `Sum.bigDecimalsPerKey()` **does not exist** in Beam — `Sum` covers `Integer`, `Long`, `Double` only. For `BigDecimal` you need a custom `Combine.perKey(...)` with your own `CombineFn`. Also, chaining `.apply(GroupByKey.create()).apply(Sum.…PerKey())` is type-broken: a `…PerKey` combiner groups internally, so after an explicit `GroupByKey` you are handing `KV<K, Iterable<V>>` to something that expects `KV<K, V>`. Use the combiner alone (`Sum.longsPerKey()`), or `GroupByKey` followed by a manual `ParDo` sum — not both.

## 11. Table–stream duality

The same data has two readings:

- **Stream** — each record is an independent, standalone fact in an unbounded set.
- **Table** — each record is an *update*; the value is the latest state for that key (a changelog).

A table is the accumulated state of a stream; a stream is the changelog of a table. Reading `("alice",1) ("charlie",1) ("alice",2) ("bob",1)` as a stream gives four facts; folding it into a keyed table gives current state `{alice:2, charlie:1, bob:1}`. This duality is the foundation of Kafka Streams' `KStream` / `KTable` split and of materialized-view systems generally.

## 12. Streaming patterns

- **Transformation** — reshape events into another format/schema/protocol on the way to a different system (Camel, ksqlDB).
- **Filters & thresholds** — pass only events meeting a predicate (`brand == "Toyota" AND year > 2010`).
- **Windowed aggregation** — shard by key, aggregate per window in parallel (sum, min, max, avg, stddev, count), rejoin.
- **Stream join** — combine events from multiple streams on a key within a window, SQL-join style.

Beyond these: temporal event ordering, buffered ordering, replay, snapshot-state persistence, failover — the reliability and scaling families. The four above are the load-bearing ones for this course.

## 13. Frameworks

| Framework | Best for | Key trait | Trade-off |
|---|---|---|---|
| Apache Flink | stateful, complex events | modern true-streaming architecture | steep learning curve |
| Kafka Streams | tight Kafka integration | embeddable library, low latency | bound to the Kafka ecosystem |
| Spark Structured Streaming | unified batch + stream | micro-batching, MLlib | latency higher than true streaming |
| Apache Beam | cross-runner pipelines | one API, many runners (Flink/Spark/Dataflow) | depends on the runner underneath |
| Google Cloud Dataflow | fully managed | serverless execution, autoscaling | GCP lock-in |

> **Correction to a common slide.** The "2013 Flink" dating is loose. What became Flink started as the *Stratosphere* research project (TU Berlin, from ~2010); it became an Apache project in 2014 and a top-level project later that year. "2013 Spark Streaming and Apache Flink" compresses two different timelines.

## 14. Connecting back to Kafka

Kafka Streams implements a practical subset of this model directly on Kafka: `KStream`/`KTable` are the table–stream duality; windowed aggregations, stream–stream and stream–table joins, and event-time processing with grace periods (Kafka's name for allowed lateness) are all here. Kafka Streams' watermark handling is simpler and stream-time-based rather than the full Dataflow trigger algebra, but the mental model is identical: *what / where / when / how*. Everything in this lesson is the theory behind the Kafka Streams API you'll use next.

> **Where this runs, and where it doesn't.** The Beam examples in this lesson run in a single JVM (DirectRunner / Playground) with no broker — deliberately, so the windowing and trigger *semantics* are visible in isolation. They do **not** show how streaming behaves distributed: across partitions, parallel workers, or state recovery. That side — the same concepts running on real topics, sharded by partition, with grace periods and state restored from changelog — is covered in the Kafka Streams lesson. lesson13 answers *what these concepts are and why*; the distributed, production version is Kafka Streams on the actual cluster.
