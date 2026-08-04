# Serialization

## 1. What serialization actually is

An object in memory is not a line of bytes. It is fields at scattered
addresses, references pointing elsewhere, layout the runtime chose. None of
that survives leaving the process. A network link carries one byte after
another; a file is bytes in a row. So before an object can travel or be
stored it must be flattened into a single ordered sequence of bytes. That
flattening is **serialization**; rebuilding the object from those bytes on
the other side is **deserialization**.

```
object in memory  ──serialize──▶  byte[]  ──network / disk──▶  byte[]  ──deserialize──▶  object
```

The word is literal: *serial* means one-after-another. Serialization puts a
scattered thing into a line.

The first thing to be clear about — because a common slide blurs it — is that
serialization is **not optional and is not, by itself, an optimization**.

> **Correction to a common slide.** Decks sometimes frame serialization as a
> technique "to reduce network load, reduce disk load, and speed things up."
> That mixes two separate things. Serialization itself is *mandatory*: without
> it an object cannot leave the process at all — there is nothing to reduce
> because nothing moves. What reduces load and adds speed is **choosing a
> compact format** (binary over text). "Do we serialize?" has one answer: yes,
> always. "Which format?" is the optimization question, and it is the real
> subject of this lesson.

## 2. Why Kafka forces you to think about it

A Kafka broker is **byte-transparent**. It stores `key` and `value` as opaque
byte arrays and never inspects them. It does not know or care whether the
bytes are JSON, Avro, Protobuf, or noise. This is deliberate — it keeps the
broker fast and simple — and it has a direct consequence: the entire meaning
of the data lives in the serializer and deserializer, not in the broker.

While producer and consumer are one codebase deployed together, this barely
matters — they share a class and never disagree. The moment the two sides
deploy independently, the bytes in the topic become the *only* contract
between them. The producer writes today; a consumer reads a week later; they
never meet. The byte format is the whole conversation. That is why
serialization is a first-class design decision in Kafka and not an
implementation detail.

Bytes carry no self-evident meaning. `01000001` is the letter `A`, or the
number `65`, or one byte of something larger — the bits do not say which. The
"common language" both sides need is not about getting bytes across (they
arrive fine); it is about agreeing how to turn them back into meaning. A
producer writing UTF-8 JSON and a consumer decoding it as a big-endian int
receive identical, perfect bytes and still get garbage.

## 3. The Serializer / Deserializer contract

Kafka expresses this as two small interfaces in
`org.apache.kafka.common.serialization`:

```java
public interface Serializer<T>   { byte[] serialize(String topic, T data); }
public interface Deserializer<T> { T deserialize(String topic, byte[] data); }
```

The producer is configured with `key.serializer` and `value.serializer`; the
consumer with `key.deserializer` and `value.deserializer`. **Key and value are
serialized independently** — a `String` key with a JSON value is the common
case.

`kafka-clients` ships serializers only for primitives: `StringSerializer`,
`IntegerSerializer`, `LongSerializer`, `DoubleSerializer`,
`ByteArraySerializer` (identity — hands the bytes through unchanged),
`ByteBufferSerializer`, `UUIDSerializer`, `VoidSerializer`. Every one has a
matching deserializer. These cover keys and trivial values. A real domain
object — `OrderCreated{orderId, amount, ...}` — needs a *format*: JSON via
Jackson, or Avro / Protobuf, discussed below.

Two consequences of the contract worth stating explicitly.

**Serialization of the key drives partitioning.** With the default
partitioner, a keyed record's partition is `murmur2(keyBytes) % partitionCount`
— computed over the *serialized* key, not the logical key. So the same logical
key serialized two different ways (`"42"` via `StringSerializer` vs `42L` via
`LongSerializer`) produces different bytes, different hashes, and can land in
**different partitions**. Two producers that disagree on key serialization
break per-key ordering, and the bug is invisible until you notice records for
one entity spread across partitions. Pick a key serializer and pin it. (This
ties back to the partitioning lecture: order is per-partition, and the key's
bytes decide the partition.)

**`null` is a legal, meaningful value.** A record with a `null` value is a
**tombstone** — on a compacted topic it marks the key for deletion. The
serializer must return `null` for `null` (the built-in ones do). A serializer
that throws or emits an empty array on `null` silently breaks compaction. When
you write a custom value serializer, handle `null` first.

## 4. Text versus binary

Two families, and the trade is consistent.

| Criterion | Text (JSON, XML, CSV) | Binary (Avro, Protobuf) |
|---|---|---|
| Readability | high — read it with your eyes, any editor, `curl` | low without tooling |
| Size | larger — field names repeated in every message, numbers as digit strings | smaller — no repeated keys, numbers packed |
| Speed | slower — string parsing, more bytes to move | faster — less to parse, less to move |
| Debugging | easy — visible in logs, no schema needed to read | needs a schema / tool to decode |
| Contract | often implicit — schema optional, drifts easily | explicit — schema drives versioning and evolution |

The essence: text favours the human (debugging, simplicity), binary favours
the machine (size, speed, a strict contract). On a Kafka stream at volume the
binary side of that trade is usually the right one — field names repeated a
million times is a million wasted copies — but for a public REST edge or an
internal admin tool the readability of JSON wins. The format is a per-context
choice, not a global verdict.

## 5. The formats

### JSON

Text. Self-describing (field names travel in every message), readable, parsed
by everything, schema not required. Cost: the largest payload of the three,
slower, and — by default — no contract at all; producer and consumer agree on
shape only by convention. In Kafka: `StringSerializer` over a
Jackson-produced JSON string, or a JSON-Schema-aware serializer (below).

### Avro

Binary, with the **schema held separately** from the data. An Avro schema is
itself written in JSON (a `.avsc` file), which is the source of a persistent
confusion.

> **Correction to a common slide.** A slide that shows an Avro *schema* — a
> JSON document with `"type":"record"` — invites the reading "Avro is JSON."
> It is not. The **schema** is JSON; the **data on the wire is binary** and
> carries no field names. That separation is exactly where Avro's compactness
> comes from: the reader already has the schema, so the bytes hold only values.

Avro's strength is **schema evolution**. The reader can use a *reader schema*
different from the *writer schema* the data was written with, and Avro
resolves between them (filling `default` values for fields the writer did not
have). This is what makes it the default for long-lived event streams. In
Kafka it is normally paired with Schema Registry (lesson 11).

### Protobuf

Binary, schema in a `.proto` file, client code generated from it. Most
compact, strongly typed, cross-language (a Google standard), and the natural
fit where `.proto` already exists (gRPC shops). Its defining idea is the
**field number**:

```proto
message OrderCreated {
  string order_id = 1;
  double amount    = 2;
}
```

The wire format identifies fields by these numbers, not by name — which is why
Protobuf is compact and why the numbers are part of the contract. Consequences
that bite:

- **Never reuse a tag number.** A retired field's number, reassigned to a new
  field, makes old bytes decode as the new field — silent corruption. Mark the
  old number `reserved`.
- Removing a field without reserving its number is the same trap.
- The `.proto` is a *transport* contract. Keep it distinct from the domain
  model; letting them merge couples your internal types to the wire.

### JSON Schema

This one is miscategorised on most slides, so state it plainly.

> **Correction to a common slide.** JSON Schema is listed alongside JSON,
> Avro, and Protobuf as if it were a fourth serialization format. It is not a
> serialization format at all. The data on the wire is still ordinary JSON.
> JSON Schema is a **validation** layer *over* JSON — a formal description of
> required fields and types, used to check a document. Its true peers are the
> Avro schema and the `.proto`, not the JSON payload. Categorise it as
> *validation for a text format*, not as a format.

Use it when JSON is already the transport but you want a machine-checkable
contract and structural validation, keeping human readability.

## 6. One event, three formats

The same domain event — `OrderCreated{orderId, amount}` — makes the trade
concrete:

- **JSON** `{"orderId":"A-1001","amount":42.5}` — ~35 bytes, readable, field
  names included, no contract.
- **Avro** — a handful of bytes, binary, needs the schema to decode,
  first-class evolution.
- **Protobuf** — the smallest, binary, field numbers as contract, generated
  code.

The three questions to ask of any event, in order: which is easiest to
**debug**, which is cheapest **on the wire**, and what happens **when a field
is added tomorrow**. The lab measures the first two in real bytes and forces
the third to happen.

## 7. Data contracts: schema is not semantics

A schema pins field names and types. It does **not** pin meaning. This gap is
where the expensive failures live.

Consider a producer team that changes `user_status` from a `STRING` with
values `"active"` / `"blocked"` to an `INTEGER` with codes `1` / `2`. They
update the schema, all their tests pass — and every downstream dashboard and
ML pipeline quietly breaks, because the *type* changed and, worse, the
*meaning* of the field changed underneath a name that stayed the same. The
schema check passed because a schema only describes structure. The contract
that was missing covered **semantics and allowed values** — which are as much
a part of the contract as the field types.

A data contract therefore has to carry more than a schema:

- **schema** — field names, types, required/optional;
- **semantics** — what each field *means*, its allowed values, its domain
  invariants;
- and, in practice, ownership and change policy.

This is why "the format has a schema" is necessary but not sufficient, and it
is the bridge to the next section.

## 8. Managed schemas: the bridge to Schema Registry

Everything above puts the entire serialization contract in the producer's and
consumer's own code. That works until schemas must **evolve** while old and
new data coexist — and in Kafka they always coexist, because **retention keeps
old messages for days or weeks**. A consumer today may read a message written
by last week's producer, in last week's shape. The writer schema and the
reader schema are genuinely different at the same instant.

Handling that by hand does not scale. **Schema Registry** (lesson 11) is the
external service that manages it: it stores schemas centrally, versions them
per subject, enforces a compatibility mode at registration, and — via the
serde wire format (`magic byte + 4-byte schema ID + payload`) — lets each
message name the exact writer schema it was written with, by ID, without
carrying the schema itself. That is how the byte-transparent broker, the
independent producer and consumer, and long retention are reconciled into a
contract that can change safely. This lesson is the *why*; lesson 11 is the
*how*.

## 9. Deviations from the ideal world

Points where a real deployment differs from the clean picture:

1. **Apache Kafka ships no serializers beyond the primitives, and no Schema
   Registry.** Avro / Protobuf serdes and the registry are separate
   dependencies (Confluent, Apicurio, Karapace). Their absence on a pure
   Apache cluster is a fact of the distribution, not a broken setup.

2. **A bad message is a poison pill.** A producer that writes a malformed or
   wrong-format value succeeds; the broker stores it; a consumer fails on
   deserialization — possibly a day later, possibly in another team's service.
   The broker cannot help; it only ever saw bytes. Deserialization errors need
   an explicit strategy (skip, dead-letter, halt), decided up front.

3. **Key and value serializers are independent and easy to mismatch.** A
   consumer configured with the wrong value deserializer reads perfect bytes
   into garbage with no warning, because — again — the broker validated
   nothing.

4. **Format is a one-way door at volume.** Changing the serialization format
   of a live topic means every consumer must handle both formats during the
   transition, for the full retention window. Choose deliberately before the
   topic has traffic.

## Summary

- Serialization (object → `byte[]`) is mandatory; *format choice* is the
  optimization. Do not conflate the two.
- The broker is byte-transparent, so the serializer/deserializer pair is the
  entire contract between an independently-deployed producer and consumer.
- Key and value serialize independently; the **key's serialized bytes decide
  the partition**, and `null` is a meaningful tombstone.
- Text (JSON) trades size and a strict contract for readability; binary (Avro,
  Protobuf) trades readability for size, speed, and a schema-driven contract.
- JSON Schema is validation over JSON, not a serialization format.
- A schema is not semantics — a full data contract also fixes meaning and
  allowed values.
- Long retention makes writer and reader schemas coexist, which is what
  Schema Registry (lesson 11) exists to manage.

The lab (`LAB.md`) makes this measurable: the same event in three formats,
their byte sizes and serialization cost side by side, the key-partitioning
hazard, schema evolution across a reader/writer split, and the `null`
tombstone — all against the lesson 6–7 broker on AWS.
