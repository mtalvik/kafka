# Lesson 12 Lab — Serialization hands-on

This lab makes the serialization trade-offs measurable. Most of it is pure
in-memory Java — serialize the same event three ways, print the bytes and
sizes, force a schema change — so it needs almost nothing from the broker.
Three exercises do touch Kafka (raw bytes through a topic, and the `null`
tombstone), running against the existing broker on the `kafka` EC2
(Apache Kafka 4.3.0, KRaft single-node, SASL/PLAIN).

Eight small Java programs (`Ex1`–`Ex8`), each illustrating one point from
`lecture.md`:

| Program | Concept | §lecture |
|---|---|---|
| `Ex1RawBytes` | Broker is byte-transparent — store bytes, get bytes back | §2 |
| `Ex2Json` | JSON: text, self-describing, field names in every message | §5 |
| `Ex3Avro` | Avro: binary, schema separate (`GenericRecord`, no codegen) | §5 |
| `Ex4Protobuf` | Protobuf: binary, field numbers as contract (`DynamicMessage`) | §5 |
| `Ex5SizeCompare` | Same event, three formats — real byte sizes + serialize time | §6 |
| `Ex6KeyPartitioning` | Key serialization decides the partition — the hazard | §3 |
| `Ex7Evolution` | Add a field: Avro writer/reader resolution vs JSON | §7 |
| `Ex8NullTombstone` | `null` value is a legal tombstone through the round-trip | §3 |

The point of the lab is not Kafka authz (that was lessons 6–7) — it is the
bytes. So it runs as the `admin` principal (super-user, no ACLs needed) and
uses one throwaway topic, `ser-demo`.

## What you will build

- A Gradle Java project (`serialization-java/`) using `kafka-clients` 4.0.0
  plus three pure-jar serde libraries — Jackson (JSON), Avro (`GenericRecord`),
  Protobuf (`DynamicMessage`). No Avro/Protobuf code-generation plugins, no
  `protoc` — so the build stays light on a t3.small.
- One runnable task per example (`gradle ex1` … `gradle ex8`).
- A `client.properties` with `admin`'s SASL/PLAIN credentials (gitignored).
- One throwaway topic, `ser-demo` (3 partitions), created in Step 1.

## Prerequisites

- The `kafka` EC2 running Apache Kafka 4.3.0 via `systemctl`, with the
  `admin` super-user (lessons 6–7).
- Gradle 8.8 on the EC2 (`/opt/gradle-8.8`, from lesson 8).
- Repo cloned on the EC2 at `~/kafka-repo/`.

## Architecture

```
  local Mac                            kafka EC2 (172.31.29.117)
  ─────────                            ──────────────────────────
  edit *.java                          ┌────────────────────────────┐
       │ git push                      │ broker localhost:9092      │
       ▼                               │   SASL/PLAIN, StandardAuthz │
  github.com/mtalvik/kafka             │   ser-demo (3 partitions)   │
       │ git pull (EC2)                └───────────▲─────────────────┘
       ▼                                           │
  ~/kafka-repo/lesson12/serialization-java         │ Producer/Consumer API
       │ gradle exN --no-daemon      ┌─────────────┴──────────────┐
       └────────────────────────────►│ java demo.ExN              │
                                      │ kafka-clients 4.0.0        │
                                      │ Jackson / Avro / Protobuf  │
                                      │ principal: admin           │
                                      └────────────────────────────┘
```

Ex2, Ex3, Ex4, Ex5, Ex6, Ex7 are pure in-memory — no broker contact. Only
Ex1 and Ex8 produce/consume against `ser-demo`.

---

## Step 1: Broker up, and create `ser-demo`

```bash
cd ~/otus-kafka
./aws-lab.sh start
./aws-lab.sh ssh kafka
```

```bash
sudo systemctl status kafka | head -3
```

Expected: `active (running)`. Create the throwaway topic as admin (3
partitions so the key-partitioning point in Ex6 is visible):

```bash
cd ~/kafka
bin/kafka-topics.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --create --topic ser-demo --partitions 3 --replication-factor 1
```

If it already exists, the command reports so — harmless.

## Step 2: Pull the repo

```bash
cd ~/kafka-repo
git pull
gradle --version | grep Gradle    # expect 8.8
```

## Step 3: Configure `client.properties`

```bash
cd ~/kafka-repo/lesson12/serialization-java
cp client.properties.example client.properties
nano client.properties     # replace <PLACEHOLDER> with admin's password
```

Find admin's password:

```bash
grep '^   password=' ~/kafka/config/kafka_server_jaas.conf
```

`client.properties` is gitignored — the password never reaches git.

```bash
export GRADLE_OPTS="-Xmx256m"
```

The first `gradle` run downloads dependencies (kafka-clients, Jackson, Avro,
Protobuf) and compiles — the heaviest build. On a t3.small run it once on
fresh CPU credits; later runs are incremental and light.

---

## Step 4: Ex2 — JSON (start here, no broker)

Run this first; it is pure in-memory and confirms the toolchain works.

```bash
gradle ex2 --no-daemon
```

Expected:

```
JSON bytes (37): {"orderId":"A-1001","amount":42.5}
round-trip: OrderCreated{orderId=A-1001, amount=42.5}
```

The payload is the readable text itself — field names `orderId` and `amount`
are part of every message. That readability is JSON's whole appeal, and its
whole cost at volume.

## Step 5: Ex3 — Avro

```bash
gradle ex3 --no-daemon
```

Expected (byte count small; hex will vary):

```
Avro schema (JSON): {"type":"record","name":"OrderCreated",...}
Avro bytes (10): 0c 41 2d 31 30 30 31 00 00 ...
round-trip: orderId=A-1001 amount=42.5
```

Two things to see: the **schema** is JSON, but the **bytes** are binary and
contain no field names — only a length-prefixed string and a packed double.
The decoder can only make sense of them *with* the schema. That separation is
where the size saving comes from.

## Step 6: Ex4 — Protobuf

```bash
gradle ex4 --no-daemon
```

Expected:

```
proto descriptor: OrderCreated { order_id=1, amount=2 }
Protobuf bytes (13): 0a 06 41 2d 31 30 30 31 11 ...
round-trip: order_id=A-1001 amount=42.5
```

The descriptor is built in code (no `.proto` file, no `protoc`) but the wire
format is identical to generated Protobuf. Note the leading `0a`: tag for
field **number 1** — Protobuf identifies fields by number, not name. That is
why the numbers are the contract.

## Step 7: Ex5 — size and speed, in numbers

The centrepiece.

```bash
gradle ex5 --no-daemon
```

Expected shape (exact numbers vary by machine):

```
format      bytes   100k serialize
JSON           37             ~25 ms
Avro           10             ~15 ms
Protobuf       13             ~12 ms

JSON is ~3.7x the Avro payload; field names are the difference.
```

The same `OrderCreated` in three formats. The byte column is the durable
point: it is the network and disk cost per message, multiplied by every
message and the whole retention window. This is the concrete version of the
text-vs-binary table in §4 — not "usually bigger," but *this many bytes*.
The timing column is CPU cost; on a record this tiny the three are close
(Jackson is heavily optimized), so read it as "same order of magnitude" and
let the size column carry the argument. The gap widens with record size and
volume.

## Step 8: Ex6 — the key-partitioning hazard

```bash
gradle ex6 --no-daemon
```

Expected shape:

```
id    String-key -> partition   Long-key -> partition   same?
40             ...                      ...              no
41             ...                      ...              yes
42             ...                      ...              no
...
=> different serialization of the SAME logical key lands in different
   partitions. Per-key ordering is broken.
```

For each id, the lab serializes the *same logical key* two ways —
`StringSerializer("42")` and `LongSerializer(42)` — and computes the target
partition the default partitioner would pick (`murmur2(keyBytes) % 3`). Most
ids land in different partitions. Two producers that disagree on key
serialization therefore scatter one entity's records across partitions, and
since order is only guaranteed per partition, ordering silently breaks. The
fix is not clever: pick one key serializer and pin it everywhere.

## Step 9: Ex7 — schema evolution

```bash
gradle ex7 --no-daemon
```

Expected:

```
--- Avro ---
wrote with v1 (orderId, amount)
read  with v2 (orderId, amount, source[default="unknown"])
resolved: orderId=A-1001 amount=42.5 source=unknown

--- JSON ---
wrote v1 {"orderId":"A-1001","amount":42.5}
read into v2 shape: orderId=A-1001 amount=42.5 source=null
```

A field (`source`) is added on the reader side. Avro resolves the writer
schema against the reader schema and supplies the `default` — a controlled,
declared outcome. JSON has no schema, so the missing field just becomes
`null`; it happens to tolerate the change, but nothing *declared* or checked
it, and a *removed* or *retyped* field would fail just as silently. This is
exactly the coexistence problem retention creates, and the reason Schema
Registry (lesson 11) manages compatibility instead of leaving it to luck.

## Step 10: Ex1 — the broker only sees bytes

```bash
gradle ex1 --no-daemon
```

Expected:

```
produced to ser-demo: {"orderId":"A-1001","amount":42.5}
consumed raw value (37 bytes): [123, 34, 111, 114, ...]
decoded as UTF-8: {"orderId":"A-1001","amount":42.5}
the broker stored and returned bytes; meaning was added by the deserializer
```

The producer sends a JSON string with `StringSerializer`; the consumer reads
the value with `ByteArrayDeserializer` — the raw bytes the broker held. The
broker neither parsed nor validated anything. The meaning appears only when
*we* decode the bytes on the read side. That is byte-transparency, made
visible.

## Step 11: Ex8 — null is a tombstone

```bash
gradle ex8 --no-daemon
```

Expected:

```
produced key=A-1001 value=null (tombstone)
consumed key=A-1001 value=null
null survived the round-trip as a real record — on a compacted topic this
deletes the key
```

The value serializer returns `null` for `null`, the broker stores a record
with a null value, and the consumer reads `value == null`. This is not an
error or an empty string — it is a tombstone, the delete marker for log
compaction (lesson 4). A value serializer that throws on `null` would break
compaction silently.

---

## What was demonstrated

| Concept | Ex | Observable result |
|---|---|---|
| Broker stores/returns opaque bytes | Ex1 | raw `byte[]` out; meaning added by client decode |
| JSON is readable text, field names repeated | Ex2 | payload *is* the readable string |
| Avro schema is JSON, data is binary | Ex3 | JSON schema, no field names in the bytes |
| Protobuf identifies fields by number | Ex4 | leading tag byte for field 1 |
| Binary is smaller than text, measurably | Ex5 | JSON ≈ 3× Avro bytes; timing side by side |
| Key serialization decides the partition | Ex6 | same logical key, two serializers, different partitions |
| Avro resolves writer↔reader; JSON just tolerates | Ex7 | Avro fills `default`; JSON yields `null` |
| `null` value is a legal tombstone | Ex8 | `null` survives the round-trip |

## Repository layout

```
lesson12/
├── lecture.md              — Serialization concepts
├── lecture.ru.md           — Russian translation
├── LAB.md                  — this file
└── serialization-java/
    ├── build.gradle        — Gradle project, one task per Ex (ex1..ex8)
    ├── settings.gradle
    ├── .gitignore          — excludes client.properties and build artifacts
    ├── client.properties.example
    └── src/main/java/demo/
        ├── OrderCreated.java      — the shared POJO (orderId, amount)
        ├── Utils.java             — connection props, topic name, serde helpers
        ├── Ex1RawBytes.java       — broker byte-transparency (touches broker)
        ├── Ex2Json.java           — JSON serialize / round-trip
        ├── Ex3Avro.java           — Avro GenericRecord serialize / round-trip
        ├── Ex4Protobuf.java       — Protobuf DynamicMessage serialize / round-trip
        ├── Ex5SizeCompare.java    — three formats: bytes + timing
        ├── Ex6KeyPartitioning.java— key serialization → partition
        ├── Ex7Evolution.java      — writer/reader schema resolution
        └── Ex8NullTombstone.java  — null value round-trip (touches broker)
```

## Cleanup

`ser-demo` is throwaway. Remove it when done, or leave it (costs nothing on an
idle broker):

```bash
cd ~/kafka
bin/kafka-topics.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config kafka-configs/clients/admin.properties \
  --delete --topic ser-demo
```

Stop the EC2 instances:

```bash
cd ~/otus-kafka
./aws-lab.sh stop
```

## Reference questions

1. In Ex5, JSON is roughly three times the Avro payload for the same event.
   Where exactly do the extra bytes go, and does that ratio grow or shrink as
   the record gains more fields?
2. In Ex6, the two serializers disagree on partition for most ids but agree
   for some. Why do a few coincide, and does that make the hazard safer?
3. In Ex7, Avro reads v1 data with a v2 reader schema and fills `source` from
   its `default`. What happens instead if v2 adds a field with **no**
   `default`, and which compatibility mode (lesson 11) forbids that?
4. In Ex1 the consumer used `ByteArrayDeserializer`. What single config change
   makes the same consumer hand you back the JSON string directly, and what
   does the broker do differently as a result? (Trick question.)
5. Ex8 produced a `null` value as a tombstone. On a topic with
   `cleanup.policy=delete` rather than `compact`, what does that same `null`
   record mean?
6. All examples run as `admin`. If instead you ran as `alice` with only
   `Write` + `Describe` on `ser-demo`, which exercises would fail, and at
   which call?
