# Lesson 10 Lab — Transactions and Exactly-Once hands-on

This lab runs the Java transactional client against the existing broker
on the `kafka` EC2 (Apache Kafka 4.3.0, KRaft single-node, SASL/PLAIN).
Unlike lessons 8–9 it needs two things set up first:

1. **Broker `__transaction_state` settings** — the transactional
   producer creates this internal topic on first use; its defaults
   assume a 3-broker cluster and will hang `initTransactions()` on one
   node until fixed (Step 1).
2. **Terraform tx- infrastructure** — topics `tx-a`, `tx-b`,
   `tx-inbound`, `tx-outbound` and alice's `TransactionalID` / group /
   topic ACLs, already declared in `lesson7/gitops`. Applied in Step 3.

Eight small Java programs (`Ex1`–`Ex8`) each illustrate one concept
from `LECTURE.md`:

| Program | Concept | §LECTURE |
|---|---|---|
| `Ex1Problem` | Idempotence OFF — duplicate on lost ack (needs disruption) | §2 |
| `Ex2Idempotent` | Idempotence ON — PID+Seq dedup, same disruption stays clean | §2 |
| `Ex3InvalidConfig` | `idempotence=true` + `max.in.flight=6` → `ConfigException` | §2 |
| `Ex4ProducerError` | Application-level resend — idempotence does not cross sessions | §2 |
| `Ex5Transaction` | Atomic write to two topics | §4 |
| `Ex6ReadWrite` | Read-process-write EOS loop, `sendOffsetsToTransaction` | §4 |
| `Ex7IsolationLevel` | `read_committed` vs `read_uncommitted`, LSO | §7 |
| `Ex8Fenced` | Two producers, one `transactional.id` → `ProducerFencedException` | §6 |

## What you will build

- A Gradle Java project (`transactions-java/`) using `kafka-clients`
  4.0.0, one runnable task per example (`./gradlew ex1` … `ex8`).
- Four throwaway topics (`tx-a`, `tx-b`, `tx-inbound`, `tx-outbound`)
  and alice's transactional ACLs — all from `lesson7/gitops`.
- A `client.properties` with alice's SASL/PLAIN credentials
  (gitignored). alice is the single principal for the whole lab.

## Prerequisites

- The `kafka` EC2 running Apache Kafka 4.3.0 via `systemctl`, with
  alice/bob SASL users (lessons 6–7).
- Gradle 8.8 on the EC2 (`/opt/gradle-8.8`, installed in lesson 8).
- Terraform runnable on the EC2 against `172.31.29.117:9092` with the
  `lesson7/gitops` state (lesson 7).
- Repo cloned on the EC2 at `~/kafka-repo/`.

## Architecture

```
  local Mac                            kafka EC2 (172.31.29.117)
  ─────────                            ──────────────────────────
  edit *.java, *.tf                    ┌────────────────────────────┐
       │ git push                      │ broker localhost:9092      │
       ▼                               │   SASL/PLAIN, StandardAuthz │
  github.com/mtalvik/kafka             │   __transaction_state (RF=1)│
       │ git pull (EC2)                │   Transaction Coordinator   │
       ▼                               │   tx-a tx-b tx-inbound ...  │
  ~/kafka-repo/                        └───────────▲─────────────────┘
    lesson7/gitops  ── terraform apply ────────────┤ (topics + ACLs)
    lesson10/transactions-java                     │ Producer/Consumer API
       │ ./gradlew exN --no-daemon    ┌────────────┴───────────────┐
       └─────────────────────────────►│ java demo.ExN              │
                                       │ kafka-clients 4.0.0        │
                                       │ principal: alice           │
                                       └────────────────────────────┘
```

Everything runs on the `kafka` EC2 against `localhost:9092`. No
cross-EC2 networking.

---

## Step 1: Broker — enable single-node transactions

The transactional producer needs the internal `__transaction_state`
topic. Its replication defaults (RF=3, min.ISR=2) cannot be satisfied
by one broker, so `initTransactions()` would block until timeout. Set
both to 1.

SSH in and edit the KRaft config used by the systemd unit:

```bash
cd ~/otus-kafka
./aws-lab.sh start
./aws-lab.sh ssh kafka
```

```bash
grep -E 'transaction.state.log|offsets.topic.replication' ~/kafka/config/server.properties
```

If the transaction lines are missing or not 1, add/fix them:

```bash
cat >> ~/kafka/config/server.properties <<'EOF'
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
EOF

sudo systemctl restart kafka
sudo systemctl status kafka | head -3
```

Expected: `active (running)`. `offsets.topic.replication.factor=1` was
already set in lesson 7; the two transaction settings are separate and
must be set explicitly.

> If `__transaction_state` was already auto-created earlier with the
> wrong RF, the setting change alone will not fix the existing topic.
> Check with `kafka-topics.sh --describe --topic __transaction_state`;
> if it exists with RF≠1 on a fresh lab broker, delete it and let it be
> recreated. On a clean broker this does not apply.

## Step 2: Pull the repo

```bash
cd ~/kafka-repo
git pull
```

Gradle 8.8 is already on the EC2 from lesson 8 (`gradle --version` →
`8.8`). If missing, reinstall as in lesson 8 Step 2.

## Step 3: Apply the tx- topics and ACLs (Terraform)

The topics and ACLs are already declared in `lesson7/gitops`
(`topics.tf` → `tx_a`, `tx_b`, `tx_inbound`, `tx_outbound`; `acls.tf` →
alice `TransactionalID`/group/topic prefixed `tx-`, bob read on `tx-`).
Apply:

```bash
cd ~/kafka-repo/lesson7/gitops
terraform plan     # review: 4 topics + tx- ACLs to add
terraform apply
```

Expected plan: the four `tx-*` topics and the alice/bob `tx-` ACLs are
**added**; nothing existing is destroyed. Confirm with `yes`.

Verify:

```bash
cd ~/kafka
bin/kafka-topics.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties --list | grep '^tx-'
```

Expected: `tx-a`, `tx-b`, `tx-inbound`, `tx-outbound`.

## Step 4: Configure `client.properties`

```bash
cd ~/kafka-repo/lesson10/transactions-java
cp client.properties.example client.properties
nano client.properties     # replace <PLACEHOLDER> with alice's password
```

Find alice's password:

```bash
grep user_alice ~/kafka/config/kafka_server_jaas.conf
```

`client.properties` is gitignored — the password never reaches git.

```bash
export GRADLE_OPTS="-Xmx256m"
```

---

## Step 5: Ex3 — invalid configuration (quick sanity check)

Run this first; it is the fastest and confirms the client library and
config path work.

```bash
gradle ex3 --no-daemon
```

Expected:

```
ConfigException as expected: Must set retries to non-zero when using the idempotent producer.
```
(or a message naming `max.in.flight.requests.per.connection`). The
point: an invalid idempotence config is rejected at construction, not
at send.

## Step 6: Ex5 — atomic multi-topic write

```bash
gradle ex5 --no-daemon
```

Expected:

```
committed transaction 0
committed transaction 1
committed transaction 2
committed transaction 3
read_committed sees tx-a: a-0 at offset 0
read_committed sees tx-a: a-1 at offset 2
read_committed sees tx-a: a-2 at offset 4
read_committed sees tx-a: a-3 at offset 6
```

Four transactions each write one record to `tx-a` and one to `tx-b`.
The `read_committed` consumer sees all four `tx-a` records — every
transaction committed. Note the offsets jump by 2: each committed
transaction also writes a commit marker, which consumes an offset in
the log (the marker itself is not delivered).

## Step 7: Ex6 — read-process-write, exactly once

```bash
gradle ex6 --no-daemon
```

Expected:

```
transaction committed for 2 record(s)
tx-outbound: k1=first-processed
tx-outbound: k2=second-processed
```

The transformer consumed two records from `tx-inbound`, produced their
processed forms to `tx-outbound`, and committed the input offsets **in
the same transaction** as the output. Each outbound record appears
exactly once. Re-run: because the offsets were committed
transactionally, the transformer does not reprocess — `tx-outbound`
does not grow.

## Step 8: Ex7 — isolation.level and LSO

The centrepiece. Run and read the interleaved output carefully:

```bash
gradle ex7 --no-daemon
```

Expected shape:

```
  read_uncommitted : 0
  read_committed   : 0
  read_uncommitted : 1
  read_uncommitted : 2
  read_uncommitted : 3
>>> commitTransaction
  read_committed   : 1
  read_committed   : 2
  read_committed   : 3
  read_uncommitted : 4
>>> abortTransaction
  read_uncommitted : END
  read_committed   : END
```

Read it against §7:

- `0` (plain, before the transaction opened) — both consumers see it
  immediately.
- `1`, `3` (inside the transaction) — only `read_uncommitted` sees them
  before the commit.
- `2` (plain, but sent **after** the transaction opened) —
  `read_uncommitted` sees it at once; `read_committed` does **not**,
  because it sits behind the Last Stable Offset until the open
  transaction resolves. This is the correction to the common slide.
- After `commitTransaction`, `read_committed` receives `1, 2, 3`
  together — including the plain `2`.
- `4` is in a transaction that aborts — `read_uncommitted` sees it,
  `read_committed` never does.

## Step 9: Ex8 — fencing (zombie protection)

```bash
gradle ex8 --no-daemon
```

Expected:

```
producer1 committed
producer2 committed (epoch bumped, producer1 now fenced)
ProducerFencedException as expected: producer1 is fenced, must be recreated
```

Two producers share `transactional.id = tx-ex8`. When producer2 called
`initTransactions()`, the coordinator raised the epoch. producer1 now
holds a stale epoch, so its next transaction is rejected with
`ProducerFencedException` — exactly how a resurrected zombie is stopped
from writing.

## Step 10: Ex4 — the boundary of idempotence

```bash
gradle ex4 --no-daemon
```

Expected:

```
key 40 seen 2 times (DUPLICATE)
key 41 seen 2 times (DUPLICATE)
...
key 49 seen 2 times (DUPLICATE)
done — keys 40..49 should be duplicated; idempotence does not cross sessions
```

Idempotence was on the whole time. The duplicates appear because the
second producer session (new PID) replayed keys 40–49 — an
application-level resend, which idempotence cannot dedup. This is the
gap transactions close.

## Step 11 (optional): Ex1 / Ex2 — idempotence under disruption

These two run for ~60 s and only show their point if you disrupt the
producer→broker connection while they run, so a delivered record's ack
is lost and the client re-sends. Reproducing it needs a second SSH
session.

Terminal A:

```bash
cd ~/kafka-repo/lesson10/transactions-java
gradle ex1 --no-daemon        # idempotence OFF
```

Terminal B, a few times while Ex1 runs:

```bash
sudo ss -K dst 127.0.0.1 dport = 9092
```

`ss -K` force-closes the matching sockets, simulating a network drop.
With idempotence off, watch Terminal A for a `DUPLICATE key=...` line —
the broker could not tell the resend from a new record.

Now the same with idempotence on:

```bash
gradle ex2 --no-daemon        # idempotence ON
```

Disrupt the same way. This time no `DUPLICATE` line appears: PID +
sequence number let the broker drop the resend. `GAP` lines may appear
on both if disruption drops records entirely — that is loss under
`acks=1`, a separate concern from duplication.

> If `ss -K` is unavailable or you cannot get root, skip this step —
> Ex4 already demonstrates the duplication problem deterministically,
> and Ex5–Ex8 show the fixes. Ex1/Ex2 are the live-disruption version
> for those who want to see it happen.

## Step 12: Verify committed data as bob (read_committed)

In a second SSH session:

```bash
cat > /tmp/bob.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="bob" password="$(grep user_bob ~/kafka/config/kafka_server_jaas.conf | cut -d'"' -f2)";
isolation.level=read_committed
EOF

~/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 172.31.29.117:9092 \
  --consumer.config /tmp/bob.properties \
  --topic tx-outbound \
  --from-beginning --max-messages 2 \
  --property print.key=true
```

Expected: `k1  first-processed` and `k2  second-processed` from Ex6 —
and nothing from any aborted transaction. `rm /tmp/bob.properties`
after. bob has READ on `tx-` topics from Step 3 and READ on any group
from lesson 7.

You can also inspect the topics in Kafbat UI on the `elastic` EC2.

---

## What was demonstrated

| Concept | Ex | Observable behavior |
|---|---|---|
| Idempotence config is validated at construction | Ex3 | `ConfigException` before any send |
| Transaction is atomic across topics | Ex5 | all four `tx-a`/`tx-b` pairs commit together |
| Read-process-write is exactly once | Ex6 | `tx-outbound` gets each record once; re-run does not grow it |
| `read_committed` hides uncommitted + aborted | Ex7 | `1,2,3` appear only after commit; `4` never |
| A non-tx record can park behind the LSO | Ex7 | plain `2` withheld from `read_committed` until commit |
| Fencing stops zombies | Ex8 | `ProducerFencedException` on the stale producer |
| Idempotence does not cross sessions | Ex4 | keys 40–49 duplicated |
| Idempotence dedups library retries | Ex1/Ex2 | duplicate under disruption vanishes when enabled |

## Repository layout

```
lesson10/
├── lecture.md             — Transactions / Exactly-Once concepts
├── LAB.md                 — this file
└── transactions-java/
    ├── build.gradle       — Gradle project, one task per Ex (ex1..ex8)
    ├── settings.gradle
    ├── .gitignore         — excludes client.properties and build artifacts
    ├── client.properties.example
    └── src/main/java/demo/
        ├── Utils.java           — SASL config loader, tx- topic names
        ├── Ex1Problem.java      — idempotence off (disruption demo)
        ├── Ex2Idempotent.java   — idempotence on
        ├── Ex3InvalidConfig.java— invalid idempotence config
        ├── Ex4ProducerError.java— application-level resend
        ├── Ex5Transaction.java  — atomic multi-topic write
        ├── Ex6ReadWrite.java    — read-process-write EOS loop
        ├── Ex7IsolationLevel.java— isolation.level + LSO
        └── Ex8Fenced.java       — transactional.id fencing

Infrastructure (topics + ACLs) lives in lesson7/gitops, applied in Step 3.
```

## Cleanup

The `tx-*` topics have 1-hour retention and are throwaway. To remove
them and the ACLs, delete their blocks from `lesson7/gitops` and
`terraform apply`, or leave them — they cost nothing on an idle broker.
The `__transaction_state` settings should stay (harmless, and needed
for any future transactional lab).

Stop the EC2 instances when done:

```bash
cd ~/otus-kafka
./aws-lab.sh stop
```

## Reference questions

1. In Ex5 the `read_committed` consumer sees `tx-a` offsets 0, 2, 4, 6
   — why the gap of 2 between records rather than 1?
2. In Ex6, if you replace `sendOffsetsToTransaction(...)` with a plain
   `consumer.commitSync()`, what guarantee do you lose, and what
   failure would produce a duplicate in `tx-outbound`?
3. In Ex7, why does `read_committed` withhold the plain message `2`
   even though it was never part of any transaction? Name the offset
   that gates it.
4. In Ex8, after producer1 is fenced, what must the application do to
   keep processing — retry the transaction, or recreate the producer?
5. Ex6 sets `enable.auto.commit=false`. What breaks if it is left at
   the default `true` while committing offsets through the transaction?
6. All examples run as alice. Which single ACL, if removed, makes
   `initTransactions()` fail — and with what error?
