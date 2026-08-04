# Schema Registry

## 1. The problem it solves

A Kafka broker is byte-transparent. It stores `key` and `value` as opaque byte arrays and has no
knowledge of what is inside them — Avro, JSON, Protobuf, or garbage. This is deliberate: it keeps the
broker fast and simple. The cost is that the entire burden of the data contract falls on the producer
and consumer.

As long as producer and consumer are one codebase deployed together, this is a non-issue: they share a
DTO through a versioned library. The problem appears the moment the two sides deploy independently. A
producer changes a field, and the consumer discovers it in production, at runtime, at 3 a.m.

Schema Registry is an external arbiter of that contract. It is **not part of the broker** — it is a
separate service exposing a REST API, backed by a compacted Kafka topic named `_schemas` where every
schema physically lives.

## 2. Wire format

This is the single most important mechanical detail, and it is the one thing a REST/curl demo cannot
show you. When a producer using an Avro serializer writes a record, what goes on the wire is **not the
schema**:

```
[ 0x00 ][ 4 bytes: schema ID (big-endian int32) ][ Avro payload ]
 magic    global schema id                          data, serialized WITHOUT an embedded schema
```

- `magic byte` = `0x00` — the serde format version.
- `schema ID` — the global ID assigned by the registry.
- payload — the record serialized **without** carrying its own schema. This is where the saving is:
  a standalone Avro file embeds its schema in every file; here it does not.

The consumer reads the 5-byte prefix, calls `GET /schemas/ids/{id}` once (then caches it), receives the
**writer schema**, and deserializes. If the consumer has its own **reader schema**, Avro performs schema
resolution between writer and reader. Every compatibility mode exists to guarantee that this resolution
cannot blow up.

Take this away from the section: **the schema ID in the message identifies the writer schema — the
schema the data was written with. It is never the reader schema.** That single fact resolves half of the
confusion students bring to this topic.

> **Correction to a common slide.** Slides frequently say "the schema travels with the message." For the
> Schema Registry serde path this is false — only the 4-byte ID travels. The schema stays in the
> registry and is fetched by ID. The "schema travels" statement is true only for a raw Avro container
> file, which is a different thing.

## 3. Registry, subject, schema ID, version

Four entities that get conflated:

- **schema** — the contract itself (an Avro record definition).
- **schema ID** — global and unique across the whole registry (more precisely: across one `_schemas`
  topic). The same schema registered under two different subjects gets **one** ID.
- **subject** — a named line of versioning. Inside a subject, schemas form an ordered sequence of
  versions `1, 2, 3…`, and compatibility is configured per subject.
- **version** — the local number inside one subject.

The default naming strategy (`TopicNameStrategy`) produces **two subjects per topic**: `<topic>-key` and
`<topic>-value`. So topic `orders` has `orders-key` and `orders-value`, versioned independently. In
practice the key is often a primitive (`long`), and only `-value` evolves.

ID is global (like a certificate serial); version is local (like "which reissue of this particular CN is
this"). The CA analogy holds.

## 4. Compatibility

The mode decides **which schema change the registry will accept at registration time**. The check runs on
`register`/`check` — not at runtime.

| Mode | Rule | Allowed change | Who must upgrade first |
|---|---|---|---|
| `BACKWARD` (default) | new schema can read data written by the old one | add optional field (with `default`), **remove** a field | **consumer** |
| `FORWARD` | old schema can read data written by the new one | **add** a field, remove an optional field | **producer** |
| `FULL` | both directions | add/remove fields that have a `default` | either |
| `NONE` | no checks | anything | nobody — you're on your own |

The subtlety slides tend to blur: **direction of the check versus order of deployment.** `BACKWARD` means
a new consumer must be able to read old messages, therefore you upgrade the **consumer first**.
`FORWARD` is the opposite. This is counter-intuitive and worth walking through: under `BACKWARD` you may
remove a field but you may **not** add a mandatory field (one without a `default`), because a new
consumer meeting an old message would have no value to supply for it.

`_TRANSITIVE` variants (`BACKWARD_TRANSITIVE`, etc.): plain `BACKWARD` checks the new schema only against
the **latest** version; transitive checks it against **all** previous versions. More expensive, but
honest for long-lived topics where old messages never fully age out.

## 5. Naming strategies

The fork is: how many record types live in one topic.

- `TopicNameStrategy` (default) — one type per topic. Subject = `<topic>-value`.
- `RecordNameStrategy` — subject = the record's fully-qualified name. Several event types in one topic
  (event sourcing: `OrderCreated` / `OrderShipped` / `OrderCancelled` on the same partition, keyed the
  same way, to preserve ordering). The same record in different topics maps to **one** global line of
  evolution.
- `TopicRecordNameStrategy` — subject = `<topic>-<record>`. Multi-type, but evolution is scoped per
  topic; the same record in topic A and topic B may diverge.

The bridge to earlier material: `RecordNameStrategy` does not exist to tidy up names. It exists to let
heterogeneous event types share **one partition with preserved order**. That ties directly back to the
partitioning lecture.

Key and value strategies are set independently (`key.subject.name.strategy`,
`value.subject.name.strategy`); in practice only the value strategy is ever changed.

## 6. Serde flow, end to end

**Producer:** object → serde inspects the schema → `POST /subjects/{subject}/versions` (or takes the ID
from cache) → receives ID → writes `magic + ID + payload`.

**Consumer:** reads the 5-byte prefix → `GET /schemas/ids/{id}` (cache) → resolution writer↔reader →
object.

Both sides cache. After warm-up the registry is not in the hot path and does not cap throughput.

## 7. Topology: HA versus multiple registries

Two different meanings of "many," routinely conflated:

**Many instances = one logical registry (HA).** N Schema Registry processes behind a load balancer, all
reading and writing **the same `_schemas` topic**. Only the **leader** performs writes; followers accept
a registration and forward it to the leader. All instances must share the same registry group and the
same `_schemas`, or you get split-brain. The client may list several URLs in `schema.registry.url`, but
that is failover **within one** registry. The ID space is single: ID 42 means the same thing on every
instance.

**Many independent registries.** Separate registries per environment (dev/stg/prod), per region, or per
domain. Each has its **own `_schemas`**, therefore its **own independent ID space.**

> **Correction to a common slide.** A schema ID is unique within one `_schemas` topic, **not** globally
> across the universe. The same Avro schema may be ID 42 in dev and ID 7 in prod. Consequently you
> **cannot** promote messages between environments by ID — on the far side that ID means a different
> schema, or none. On migration you re-register the schema and the serde supplies the local ID.

Confluent addresses cross-registry movement with **Schema Linking** (replicating subjects between
registries, e.g. prod → DR) and **IMPORT mode** (registering while preserving the source ID so the ID
spaces line up after migration).

Registry-to-cluster coupling exists only through the `_schemas` storage topic, which yields two
topologies: one registry serving several application clusters (shared governance, single blast radius),
or one registry per cluster (isolation, at the cost of duplicated schemas and ID spaces).

## 8. Deviations from the ideal world

Points where a real deployment differs from the clean textbook picture:

1. **The `_schemas` replication factor default assumes a cluster.** Confluent Schema Registry defaults
   `kafkastore.topic.replication.factor` to 3. On a single-node broker, creating that topic hangs.
   Set it to `1`.

   > **Correction to a common slide.** This is the same class of bug as `__transaction_state` on a
   > single broker: an internal topic whose default replication factor exceeds the number of brokers,
   > silently blocking startup until you override it.

2. **Schema Registry is a client of the broker.** On a SASL/PLAIN cluster it needs its own principal and
   ACLs (produce/consume on `_schemas`, plus its consumer group). Provision that as code, not by hand.

3. **Apache Kafka does not ship a Schema Registry.** It must be installed separately — Confluent Schema
   Registry (standalone), Apicurio, or Karapace. On a pure Apache cluster its absence is a fact of the
   distribution, not a broken installation.
