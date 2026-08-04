# Lab 15 — Kafka Streams

All commands run on the `kafka` EC2 unless stated otherwise. The single-node
broker uses SASL/PLAIN; every client authenticates as `bob`, the same
principal used in lesson 14. Exercises read and write Kafka topics only —
there is no separate Streams process to deploy, the app runs inside
`gradle exN`.

## 0. Prerequisites

```bash
ssh-add ~/.ssh/id_ed25519_mtalvik          # on the Mac, before scp/ssh
# on the kafka EC2 (~/kafka-repo is the repo clone; ~/kafka is the broker install):
cd ~/kafka-repo/lesson15/streams-java
git -C ~/kafka-repo pull
cp client.properties.example client.properties
# set the bob password:
sed -i "s|REPLACE_ME|$(grep user_bob ~/kafka/config/kafka_server_jaas.conf | cut -d'"' -f2)|" client.properties
```

The console tools need the same credentials:

```bash
cat > /tmp/admin.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="bob" password="$(grep user_bob ~/kafka/config/kafka_server_jaas.conf | cut -d'"' -f2)";
EOF
```

Gradle is installed system-wide on this host (8.8, JDK 17, since lesson 8) —
there is no wrapper in the repo. t3.small is memory-tight, so cap the JVM
before building:

```bash
export GRADLE_OPTS="-Xmx256m"
gradle build --no-daemon
```

## 1. Topics and ACLs

Managed by Terraform in `lesson7/gitops`, the same GitOps workflow as lesson
7. Not optional here: the topologies run as `bob`, who has no Create
permission on the cluster, so creating these topics by hand under that
principal fails.

```bash
cd ~/kafka-repo/lesson7/gitops
terraform apply
cd ~/kafka-repo/lesson15/streams-java
```

Thirteen topics, all `replication_factor = 1`. Every one is single-partition
except `purchases`, which has **2** — deliberate, and step 5 depends on it.

> Streams creates its own internal topics (changelog + repartition) at
> startup, named `lesson15-exN-...`. Those are not in Terraform: the app
> creates them, which is why `bob` holds a prefixed `lesson15-` ACL that
> includes Create. Their replication factor comes from
> `StreamsConfig.REPLICATION_FACTOR_CONFIG`, which `Utils.streamProps` pins to
> 1. If an app hangs on start with a topic-creation error, that setting was
> lost.
>
> The consumer group is named after `application.id`; `bob` has Read on Group
> `*` from lesson 7, so no extra grant is needed.

## 2. Ex1 — upper-case topology

Shell variables used from here on:

```bash
BS=localhost:9092
CFG=/tmp/admin.properties
```

Terminal A:
```bash
gradle ex1 --no-daemon
```
Terminal B — produce, then read the output topic:
```bash
echo "hello streams" | ~/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server $BS --producer.config $CFG --topic src-topic

~/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $CFG \
  --topic out-topic --from-beginning --max-messages 1
```
Expected: `HELLO STREAMS` on `out-topic`, and an `upper:` line in terminal A.
`Ctrl-C` the app when done.

## 3. Ex2 — mask and fan out

Purchase CSV: `customerId,employeeId,department,amount,card`.

```bash
gradle ex2 --no-daemon      # terminal A
# terminal B:
echo "c1,e7,electronics,250.00,4111111111111234" | \
  ~/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS --producer.config $CFG --topic purchases
```
Check the sinks:
```bash
for t in purchases-masked rewards patterns; do
  echo "== $t =="; ~/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BS \
    --consumer.config $CFG --topic $t --from-beginning --max-messages 1; done
```
Expected: masked card ends `****1234`; `rewards` shows `c1,25`; `patterns`
shows `electronics`.

## 4. Ex3 — filter, split, foreach

```bash
gradle ex3 --no-daemon
# produce a mix:
printf '%s\n' \
  "c1,e7,cafe,8.50,4111111111111234" \
  "c2,e9,electronics,900.00,5555444433332222" \
  | ~/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS --producer.config $CFG --topic purchases
```
Expected: `cafe-sales` gets the cafe row, `electronics-sales` gets the
electronics row, `expensive-purchases` gets the > 100 row (keyed by `c2`), and
terminal A prints a `persist employee=...` line per record.

## 5. Ex4 — stateful reward accumulation

```bash
gradle ex4 --no-daemon
# same customer several times:
printf '%s\n' \
  "c1,e7,cafe,50.00,x" "c1,e7,electronics,120.00,x" "c1,e7,cafe,30.00,x" \
  | ~/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS --producer.config $CFG --topic purchases
```
Expected: `BonusByCustomer` lines with a running total for `c1` (5, then 17,
then 20). Confirm the state is durable:
```bash
~/kafka/bin/kafka-topics.sh --bootstrap-server $BS --command-config $CFG --list | grep lesson15-ex4
```
You should see `lesson15-ex4-rewards-store-changelog` and the
`lesson15-ex4-reward-by-customer-repartition` topic. The repartition topic is
why `c1` stays on one Task even though `purchases` has 2 partitions and no
input key.

## 6. Ex5 — windowed join

```bash
gradle ex5 --no-daemon
# same customer in both streams within 20 min (key:value, parse.key=true):
echo "c1:tv" | ~/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS \
  --producer.config $CFG --topic electronics-events \
  --property parse.key=true --property key.separator=:
echo "c1:latte" | ~/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS \
  --producer.config $CFG --topic cafe-events \
  --property parse.key=true --property key.separator=:
```
Expected: a `coupon: customer=c1 -> free-coffee-coupon` line, and a record on
`coupons`. A customer appearing in only one stream produces nothing (inner
join).

## 7. Ex6 — session count (homework)

```bash
gradle ex6 --no-daemon
~/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS --producer.config $CFG \
  --topic events --property parse.key=true --property key.separator=:
```
Type keyed events, one per line:
```
a:1
a:2
a:3
```
Expected: a line roughly a second after each event, not after the 5-minute
gap — `commit.interval.ms` is 1000, so each update to the session is flushed
and emitted as it happens:

```
key=a window=[... .. ...] count=1
key=a window=[... .. ...] count=2
key=a window=[... .. ...] count=3
```

The window `endTime` moves forward with each event: a session window is
bounded by its first and last record, not by a fixed size. The count is
cumulative within one session, so the third line reads 3, not 1.

Now wait more than 5 minutes and send `a:4`. It opens a **new** session with
`count=1` and a fresh window start — that gap is the whole mechanism. To see
it without waiting, drop the gap to `Duration.ofSeconds(20)` in
`Ex6SessionCount`, reset the app (step 8), and rerun.

Sending a different key does not close key `a`'s session. Session results are
emitted as they update, not on close, so there is nothing to trigger.

## 8. Reset and cleanup

To rerun an exercise from scratch, stop it and reset its state and internal
topics:

```bash
~/kafka/bin/kafka-streams-application-reset.sh --bootstrap-server $BS \
  --config-file $CFG --application-id lesson15-ex6 --input-topics events
rm -rf state/lesson15-ex6
```

When done for the day:

```bash
rm -f /tmp/admin.properties client.properties
```

The lab topics are Terraform-managed — do not delete them with
`kafka-topics.sh`, or the next `terraform plan` will want to recreate them. To
tear the lab down, remove the lesson 15 blocks from `lesson7/gitops/topics.tf`
and `acls.tf` and apply.

Deliverable for the homework: the `events` topic, `Ex6SessionCount`, and a
console-producer session showing same-key events counted within the 5-minute
window.
