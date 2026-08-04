# Lesson 11 — Schema Registry — LAB

All commands run on AWS EC2. Nothing runs on the Mac.

## Topology

```
kafka   EC2 (t3.small)  Apache Kafka 4.3.0, KRaft single-node, SASL/PLAIN
                         private IP 172.31.29.117:9092
                         -> holds the _schemas topic

elastic EC2 (t3.small)  Docker + existing monitoring/UI compose stack
                         -> we add the schema-registry container here, REST on :8081

clients / kafka EC2      Gradle 8.8 at /opt/gradle-8.8
                         -> runs the Java Avro producer/consumer (schema-registry-java/)
```

Schema Registry runs on `elastic` and connects to the broker on `kafka` as a SASL/PLAIN client.
Confirm the security group `otus-kafka-lab-sg` allows `elastic -> kafka:9092` inside the VPC before
starting; both instances are in `eu-north-1a`, same VPC.

---

## Part 0. Prerequisites

Register the broker's private DNS / IP once so the compose file and clients agree. Everything below uses
`172.31.29.117:9092` as the broker's internal SASL/PLAIN listener.

Schema Registry is a **client of the broker**. For the lab it authenticates as an existing SASL user
(`alice`). In production it gets its own principal and ACLs (produce/consume on `_schemas` plus its
consumer group), provisioned as code in `lesson7/gitops/` — not reused from a human user.

---

## Part 1. Run Schema Registry on elastic

Add the service to the existing compose on `elastic`. Do **not** bundle a broker — the broker already
runs on `kafka`.

```yaml
# add to the compose stack on the elastic EC2
schema-registry:
  image: confluentinc/cp-schema-registry:7.6.1
  container_name: schema-registry
  hostname: schema-registry
  ports:
    - "8081:8081"
  environment:
    SCHEMA_REGISTRY_HOST_NAME: schema-registry
    SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081

    # point at the real broker on the kafka EC2 (not a bundled one)
    SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: SASL_PLAINTEXT://172.31.29.117:9092

    # THE single-node trap: default is 3, which hangs on a one-broker cluster.
    SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR: 1

    # SR authenticates to the broker as a SASL/PLAIN client
    SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL: SASL_PLAINTEXT
    SCHEMA_REGISTRY_KAFKASTORE_SASL_MECHANISM: PLAIN
    SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG: >-
      org.apache.kafka.common.security.plain.PlainLoginModule required
      username="alice" password="alice-secret";
```

> If your broker listener is `SASL_SSL` (as built in lesson6, not `SASL_PLAINTEXT`), switch
> `SECURITY_PROTOCOL` to `SASL_SSL` and add the truststore lines from the Appendix.

Bring it up and wait for readiness:

```bash
docker compose up -d schema-registry
# empty array = registry is up, no schemas yet
curl -s http://localhost:8081/subjects
# []
```

Confirm the storage topic was created with replication factor 1 (from the kafka EC2):

```bash
~/kafka/bin/kafka-topics.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config client.properties \
  --describe --topic _schemas | grep ReplicationFactor
# ReplicationFactor: 1
```

---

## Part 2. REST warm-up

Reuse the Avro schemas already in `schema-registry-rest-demo/schemas/` (`user-v1`, `user-v2-compatible`,
`user-v3-incompatible`). Run from `elastic` (or tunnel `:8081`). Subject: `users-value`.

Register v1:

```bash
curl -s -X POST http://localhost:8081/subjects/users-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @register-user-v1.json
# {"id":1}
```

Inspect:

```bash
curl -s http://localhost:8081/subjects
# ["users-value"]

curl -s http://localhost:8081/subjects/users-value/versions/latest
# {"subject":"users-value","version":1,"id":1,"schema":"..."}
```

Note `id` (global) vs `version` (local to the subject) — they are not the same number.

Check a compatible change (add `email` with a `default`) before registering it:

```bash
curl -s -X POST http://localhost:8081/compatibility/subjects/users-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @register-user-v2-compatible.json
# {"is_compatible":true}

curl -s -X POST http://localhost:8081/subjects/users-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @register-user-v2-compatible.json
# {"id":2}
```

Check an incompatible change (`id` int -> string):

```bash
curl -s -X POST http://localhost:8081/compatibility/subjects/users-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @register-user-v3-incompatible.json
# {"is_compatible":false}
```

Compatibility config:

```bash
curl -s http://localhost:8081/config
# {"compatibilityLevel":"BACKWARD"}
```

The REST layer proves the registry works and enforces contracts. It does **not** show the wire format —
that is Part 3.

---

## Part 3. Java Avro producer/consumer — the wire format

Move to the Gradle project `schema-registry-java/` on the machine with Gradle 8.8. This is where the
`magic byte + 4-byte schema ID` prefix becomes visible.

```bash
cd schema-registry-java
gradle ex1Produce     # produce one Avro record via the Confluent serde
gradle ex2Consume     # consume it back, deserialized via schema fetched by ID
```

`ex1Produce` also prints the raw bytes of the produced value so the 5-byte prefix is visible:

```
value bytes: 00 00 00 00 01 0c 41 6c 69 63 65 ...
             ^magic ^-- schema id = 1 --^ ^-- Avro payload --^
```

Compare the on-wire size against the same record as JSON — the Avro payload carries no field names, so it
is several times smaller. Record both numbers; this is the concrete argument for Avro + registry over
raw JSON.

---

## Part 4. Schema evolution — compatible change

`ex3Evolve` produces a record with `User` v2 (adds `email` with a `default`). Under `BACKWARD` a new
consumer must read old messages, so the upgrade order is **consumer first**.

```bash
gradle ex2Consume     # old consumer, still reading — should keep working
gradle ex3Evolve      # produce v2 records
```

Point to observe: the old consumer (reader schema = v1) reads v2 messages fine because Avro schema
resolution drops the unknown `email` field. A v2 consumer reading v1 messages fills `email` from its
`default`. Both directions of the `BACKWARD` guarantee are visible here.

---

## Part 5. Break compatibility

`ex4Break` attempts to register `User` with `id` changed from `int` to `string`.

```bash
gradle ex4Break
# registration is rejected by the registry: incompatible schema
```

The producer never gets a schema ID, so no message with the broken schema can be written. This is the
whole point of the registry: the break is caught at registration, not discovered by a consumer at
runtime. Contrast with `NONE` compatibility, where the same call would succeed and move the failure into
production.

---

## Part 6. (Optional) RecordNameStrategy — multiple types on one topic

`ex5MultiType` configures `value.subject.name.strategy=RecordNameStrategy` and produces two record types
(`OrderCreated`, `OrderShipped`) to a single topic, keyed the same way so they land on the same partition
in order.

```bash
gradle ex5MultiType
curl -s http://localhost:8081/subjects
# subjects are now the fully-qualified record names, not <topic>-value
```

This is the mechanical reason `RecordNameStrategy` exists: heterogeneous event types sharing one
partition with preserved order — the same ordering concern from the partitioning lesson.

---

## Appendix

### A. SASL_SSL broker variant

If the broker listener is `SASL_SSL`, add to the schema-registry service environment:

```yaml
    SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL: SASL_SSL
    SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_LOCATION: /etc/sr/truststore.jks
    SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_PASSWORD: changeit
```

and mount the truststore built in lesson6 into the container.

### B. Why `_schemas` replication factor must be 1

Same class of bug as `__transaction_state` in lesson10: an internal topic whose default replication
factor (3) exceeds the number of brokers (1). Without the override the topic cannot be created and the
registry hangs on startup. Set `KAFKASTORE_TOPIC_REPLICATION_FACTOR=1` on a single-node broker.

### C. Cleanup

```bash
# stop the registry container on elastic
docker compose stop schema-registry
docker compose rm -f schema-registry

# to reset registered schemas, delete the storage topic on the kafka EC2
~/kafka/bin/kafka-topics.sh --bootstrap-server 172.31.29.117:9092 \
  --command-config client.properties --delete --topic _schemas
```
