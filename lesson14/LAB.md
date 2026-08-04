# Lab 14 — Kafka Streams DSL

All commands run on the `kafka` EC2 (KRaft, single node, SASL/PLAIN). Nothing runs on the Mac. Edit on the Mac, `git push`, then on the broker `git pull` and build/run there.

## 0. Prerequisites

```bash
# on the kafka EC2
# ~/kafka-repo is the repo clone; ~/kafka is the broker install
cd ~/kafka-repo
git pull
cd lesson14/streams-java
```

The Gradle wrapper is not checked in — the `kafka` EC2 has Gradle 8.8 and JDK 17 system-wide since lesson 8, so use `gradle` directly:

```bash
gradle --version                 # expect Gradle 8.8, JVM 17
```

t3.small is memory-tight; cap the JVM or the first build gets OOM-killed:

```bash
export GRADLE_OPTS="-Xmx256m"
gradle build --no-daemon
```

## 1. Topics and ACLs

Topics and ACLs for this lab are managed by Terraform in `lesson7/gitops`, not created by hand. This is the same GitOps workflow from lesson 7, and it is not optional here: the Streams app runs as `bob`, and `bob` has no Create permission on the cluster, so `kafka-topics.sh --create` under that principal fails.

```bash
cd ~/kafka-repo/lesson7/gitops
terraform apply
cd ~/kafka-repo/lesson14/streams-java
```

This creates `stock-ticks`, `stock-transactions`, `top-shares`, `windowed-counts`, `transaction-summary`, `enriched-summary` (2 partitions each) plus the compacted GlobalKTable sources `companies` and `customers`, and grants `bob` Read/Write/Describe on all of them.

Every topic gets `replication_factor = 1` — not optional on a single broker. The internal changelog and repartition topics Streams creates at runtime inherit `replication.factor` from the app config instead, which is why `Config` sets it to 1 (see lecture section 8). Those internal topics are covered by a prefixed `lesson14-` ACL that also grants Create, since the app creates them itself.

## 2. Client config (SASL)

The console tools and the Streams app authenticate as a principal. We reuse `bob`. Put this in `~/client.properties`:

```properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="bob" password="bob-pass";
```

Set `BS` and `CC` for the console commands below:

```bash
BS=localhost:9092
CC=~/client.properties
```

The Streams app reads the same values from `src/main/resources/streams.properties` (already wired; adjust `bootstrap.servers` to the broker's private IP if you run the app from another host — from the broker itself `localhost:9092` is fine).

ACLs for `bob` are already applied by step 1. A Streams app is consumer, producer and admin client at once, so beyond the obvious read/write it needs Create on the prefixed internal topics and Read on the consumer group named after `application.id` — `bob` holds Read on Group `*` from lesson 7.

## 3. ex1 — KTable and caching

Goal: see the stream/table duality and the cache dedup from lecture section 3.

Seed a few updates for two tickers (note repeated keys):

```bash
~/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS --producer.config $CC \
  --topic stock-ticks --property parse.key=true --property key.separator=:
YERB:105.24
NDLE:33.56
YERB:105.36
YERB:105.40
```

Run:

```bash
gradle ex1 --no-daemon
```

`Ex1KTable` builds a `KTable` over `stock-ticks` and logs every value it forwards downstream. Run once with caching on (default) and once with `statestore.cache.max.bytes=0` (pass `-Pcache=0`). With cache 0 you see every YERB update; with the default cache the intermediate `105.36` is deduplicated away and only the latest per flush is emitted. That difference is the whole point.

## 4. ex2 — Aggregation and top-3 per industry

Goal: sections 4 (aggregation) and the KGroupedTable subtractor.

Seed transactions (value is JSON `StockTransaction`):

```bash
~/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS --producer.config $CC \
  --topic stock-transactions
{"ticker":"YERB","industry":"tea","shares":1000}
{"ticker":"NDLE","industry":"tea","shares":400}
{"ticker":"OCHK","industry":"metals","shares":900}
{"ticker":"YERB","industry":"tea","shares":934}
```

Run:

```bash
gradle ex2 --no-daemon
```

`Ex2Aggregation` maps each transaction to a `ShareVolume`, groups by ticker and reduces to total shares per company (`KTable<String, ShareVolume>`), then groups **that table** by industry and aggregates into a fixed top-3 per industry. Because the second grouping is over a table, the aggregate uses an adder **and** a subtractor — when a company's total updates, its old contribution is removed before the new one is added. Output lands in `top-shares` as `YERB:1934`, `OCHK:900`, etc.

```bash
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $CC \
  --topic top-shares --from-beginning
```

## 5. ex3 — Windowed counts with suppression

Goal: section 5 — tumbling window, grace, suppress.

```bash
gradle ex3 --no-daemon
```

`Ex3Window` keys `stock-transactions` by `ticker`, counts per **10-second tumbling** window with a 5-second grace, and `suppress(untilWindowCloses)` so exactly one final count per window is emitted rather than a stream of partials. Produce a burst, wait past window close + grace, and read:

```bash
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $CC \
  --topic windowed-counts --from-beginning \
  --property print.key=true
```

Keys print as `ticker@windowStart/windowEnd`. If you change the window type, keep the four names straight: this is tumbling (advance = size), not sliding.

## 6. ex4 — GlobalKTable enrichment join

Goal: section 7 — enrich a stream against two GlobalKTables with no repartitioning.

Seed the reference data:

```bash
~/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS --producer.config $CC \
  --topic companies --property parse.key=true --property key.separator=:
YERB:Yerba Holdings
OCHK:Ochkarik Optics
```

```bash
~/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS --producer.config $CC \
  --topic customers --property parse.key=true --property key.separator=:
c-1:Alice
c-2:Bob
```

Seed the summary stream (value is JSON `TransactionSummary`):

```bash
~/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS --producer.config $CC \
  --topic transaction-summary
{"customerId":"c-1","stockTicker":"YERB","industry":"tea","summaryCount":3}
{"customerId":"c-2","stockTicker":"OCHK","industry":"metals","summaryCount":1}
```

Run:

```bash
gradle ex4 --no-daemon
```

`Ex4GlobalKTable` streams `transaction-summary` and joins it against the `companies` and `customers` GlobalKTables using a `KeyValueMapper` to pick the lookup key (`stockTicker`, then `customerId`) from each record. No `selectKey`, no repartition topics — the stream keeps its own key. Output to `enriched-summary` has `customerName` and `companyName` filled:

```bash
~/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $CC \
  --topic enriched-summary --from-beginning
```

## 7. Cleanup and reset

Streams keeps local state and internal topics. To rerun an exercise from scratch, reset it (stop the app first):

```bash
~/kafka/bin/kafka-streams-application-reset.sh --bootstrap-server $BS \
  --config-file $CC \
  --application-id lesson14-ex2 \
  --input-topics stock-transactions
rm -rf /tmp/kafka-streams/lesson14-ex2
```

Lab topics are Terraform-managed — do not delete them with `kafka-topics.sh` or the next `terraform plan` will want to recreate them. To tear the lab down, remove the lesson 14 blocks from `lesson7/gitops/topics.tf` and `acls.tf` and apply. The internal `lesson14-*` topics are not Terraform-managed and are removed by the reset tool above.

## Notes

- If an app dies on startup with a replication-factor error while creating `*-changelog` or `*-repartition` topics, `replication.factor=1` is missing from its config. This is the single-node trap from lecture section 8.
- Internal topics are named `<application.id>-<store>-changelog` and `<application.id>-<name>-repartition`. The exercises use `lesson14-ex1` … `lesson14-ex4`, so one prefixed ACL covers them all; keep the ids distinct or the exercises collide.
- GlobalKTable sources (`companies`, `customers`) are read in full by every instance at startup. Keep them compacted and small.
