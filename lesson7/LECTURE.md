# Lesson 7 — Admin API and REST API

This lesson covers Kafka's administrative interfaces: the Java `Admin` API
exposed by `kafka-clients`, and the Confluent REST Proxy. The focus is on
what these APIs are for, what they can do, and how they are used in
production today — including the GitOps approach that replaces hand-written
`AdminClient` code in most modern deployments.

The course material (slide deck `06___Admin_API.pdf`, Java examples in the
upstream `OtusTeam/OTUS-Kafka` repository) demonstrates direct Java usage of
the Admin API. This lecture covers the same API surface but frames it from
an operator's perspective, with concrete production patterns from 2025–2026.

## Why an Admin API exists

Before the Admin API (Kafka < 0.11), all administrative operations were
performed via shell scripts (`kafka-topics.sh`, `kafka-acls.sh`, etc.).
These scripts wrote directly to ZooKeeper. There was no programmatic
way to manage a cluster from application code — every operator action
required shell access to a host with the Kafka distribution installed.

This was painful for several reasons:
- Tools and dashboards (Kafdrop, Conduktor, Kafka UI) had to either invoke
  shell scripts as subprocesses or speak the wire protocol directly.
- Application code that needed to create topics on demand (Kafka Streams
  internal topics, integration test fixtures) had no clean API.
- ZooKeeper coupling: any client doing admin work needed ZooKeeper
  credentials, not just Kafka credentials.

KIP-4 introduced the `AdminClient` interface in Kafka 0.11 (2017),
providing a typed Java API that speaks directly to brokers using the
Kafka wire protocol. With KRaft (Kafka 3.3+ in preview, default in 4.0)
ZooKeeper is gone entirely — the Admin API is the only way to administer
a Kafka cluster programmatically.

## Admin API surface

The `Admin` interface (in package `org.apache.kafka.clients.admin`) covers
roughly six categories:

**Topic management.** Create, delete, list, describe topics. Add partitions
(`createPartitions`). Reassign partitions across brokers
(`alterPartitionReassignments`). Inspect log directories on disk
(`describeLogDirs`, `alterReplicaLogDirs`).

**Records.** Delete records up to a given offset (`deleteRecords`). This
is a destructive operation that trims the log head; the topic itself
remains.

**Consumer groups.** List, describe, delete groups. List, alter, delete
committed offsets. This is how you implement "reset consumer to offset X"
or "reset to earliest" without restarting the consumer.

**ACLs.** Create, delete, describe access control lists. The model is
five-tuple: `principal × host × operation × resource × permission_type`.
Pattern type can be `Literal` (exact match) or `Prefixed` (wildcard prefix
match — useful for "all topics starting with `team-a-`").

**Quotas.** Per-user / per-client throughput and request-rate limits.
Set with `alterClientQuotas`, query with `describeClientQuotas`.

**Configs and cluster info.** Read and modify broker and topic
configuration (`incrementalAlterConfigs`, `describeConfigs`). Describe
the cluster (broker list, controller, cluster ID).

**Transactions** (covered in a later lesson). `describeTransactions`,
`abortTransaction`, `listTransactions`.

## API patterns

Three patterns appear throughout the Admin API:

**Asynchronous, future-based.** Every operation returns a `*Result` object
containing one or more `KafkaFuture<T>`. Synchronous code calls `.get()`
to block; reactive code uses `.toCompletionStage()` to compose. This
matters because admin operations can take seconds (partition reassignment
can take minutes) and you do not want to block a request thread.

**Batch by default.** Most methods accept a collection. `createTopics(...)`
takes a `Collection<NewTopic>`, not a single topic. `deleteAcls(...)`
takes a collection of filters. This is a deliberate API choice: a single
round-trip to the broker creates many resources, rather than one round-trip
per resource.

**Two overloads per method.** A short form with sensible defaults, and
a long form with an `*Options` object. Example:

```java
default CreateTopicsResult createTopics(Collection<NewTopic> newTopics) {
    return createTopics(newTopics, new CreateTopicsOptions());
}

CreateTopicsResult createTopics(Collection<NewTopic> newTopics,
                                CreateTopicsOptions options);
```

Use the short form for typical work; reach for `CreateTopicsOptions`
when you need `validateOnly=true` (dry-run), `retryOnQuotaViolation`, or
a non-default timeout.

The Admin interface is annotated `@InterfaceStability.Evolving`, meaning
backwards-incompatible changes can happen across minor versions. In
practice the surface is stable and changes are usually additive, but
expect occasional method signature changes when upgrading `kafka-clients`.

## How the Admin API is used in production today

Three patterns dominate modern Kafka administration. Direct `AdminClient`
code — what the OTUS course demonstrates — is the least common of the
three.

### Pattern 1: declarative GitOps (most common)

The desired state of the cluster (topics, ACLs, quotas) is described in
declarative configuration files held in Git. A reconciliation tool reads
the Git repository, queries the cluster via the Admin API, computes the
difference, and applies changes.

Two tools dominate:

- **Strimzi** (Kubernetes-native). Topics, users, and ACLs are Kubernetes
  custom resources (`KafkaTopic`, `KafkaUser`). A Topic Operator watches
  the resources and reconciles via the Admin API. Standard tooling
  (`kubectl`, ArgoCD, Flux) provides PR workflow, audit, and rollback.
- **Terraform** with the `Mongey/kafka` provider (self-managed Kafka)
  or `confluentinc/confluent` (Confluent Cloud). Topics and ACLs are
  Terraform resources. `terraform plan` shows the diff before any change;
  `terraform apply` executes it via the Admin API. State is held in
  remote backend (S3, GCS, Terraform Cloud).

This lesson's lab uses Terraform. The mechanism is the same as Strimzi
under the hood — declarative state, reconciliation, Admin API calls —
but Terraform fits non-Kubernetes deployments.

### Pattern 2: management UIs

For day-to-day operator tasks (inspecting topics, reading messages,
checking consumer lag, granting one-off ACLs) most teams use a UI.
Common choices:

- **Kafka UI** (Provectus, now Kafbat) — open source, modern UI.
- **AKHQ** — open source, JVM-based, supports Schema Registry and Connect.
- **Conduktor** — commercial, governance and policy features.
- **Confluent Control Center** — Confluent Platform / Cloud only.

These tools call the Admin API on the user's behalf. For routine work
this is faster than running Terraform; for repeatable infrastructure
changes Terraform/Strimzi is correct.

### Pattern 3: application code

Some applications legitimately use the Admin API directly:

- **Kafka Streams** auto-creates internal topics (`*-changelog`,
  `*-repartition`) on first run.
- **Integration tests** use `AdminClient` to set up fixtures against
  testcontainers / embedded Kafka.
- **Bespoke admin tools** for organizations with unusual policy
  requirements (auto-generated names, custom approval workflows) that
  do not fit Strimzi or Terraform.

The "self-service portal" pattern shown in the OTUS slides
(a custom web form that creates topics via `AdminClient`) is in this
category. It is still seen in some large organizations, but it has been
largely displaced by GitOps where Git itself becomes the request system
(open a PR, get review, merge applies).

## The REST API

Confluent REST Proxy (`cp-kafka-rest`) is a separate process that exposes
Kafka over HTTP. It speaks the Kafka wire protocol on one side and
HTTP/JSON on the other.

It exists because some clients cannot use a native Kafka client library:
browser JavaScript, IoT devices with no native librdkafka build,
languages with weak Kafka support. For these, HTTP is the only option.

### What REST Proxy provides

- **Produce/consume**: HTTP POST to send records, HTTP GET to consume.
- **Admin operations**: list/describe topics, partitions, consumer groups,
  configs, ACLs (Kafka REST API v3, modeled on the Java Admin API).
- **Schema integration**: serialize using JSON, Avro, or Protobuf via
  Schema Registry.

### Constraints

- Producer and consumer configuration is fixed at REST Proxy startup —
  individual HTTP requests cannot tune `acks`, `linger.ms`, etc.
- Consumer state lives on the REST Proxy instance, not on the cluster.
  Each consumer must be created (`POST /consumers/<group>`) and is bound
  to one Proxy instance for its lifetime. Failover requires re-creating
  the consumer.
- One topic per produce request — cannot multiplex into multiple topics
  in a single HTTP call.
- Cannot use different serializers for key and value.

### Authentication

REST Proxy authenticates clients with HTTP Basic Auth (or mTLS in
recent versions), then authenticates itself to Kafka brokers with SASL
or mTLS. Kafka ACLs apply to the principal that REST Proxy presents to
brokers — meaning all REST Proxy users typically share one broker-side
principal unless the Proxy is configured with principal propagation.

### Modern alternatives

REST Proxy is still maintained but is no longer the only option:

- **Kafka REST API v3** is now built into Confluent Server brokers
  directly. For Confluent Platform / Cloud, a separate REST Proxy
  process is not required for admin operations.
- **Kroxylicious** — Kafka-aware proxy, primarily for security and
  governance (encryption, multi-tenancy, audit).
- **Conduktor Gateway** — commercial Kafka gateway with policy
  enforcement and traffic shaping.
- **API gateways with Kafka plugins** — Kong, Gravitee, Apache APISIX.
  Useful when Kafka is one of several backends behind a unified API.

For new projects today: if you have a working native Kafka client for
your language, use it. Reach for REST Proxy only when a native client
is not viable.

## Operational recommendations

These are not strictly part of the API but matter when deploying.

**REST Proxy sizing.** Memory is bimodal: producers and admin requests
are nearly stateless (~1 GB heap), but each consumer holds buffered
messages in memory (~16 MB per active consumer). Plan for peak concurrent
consumer count, not peak request rate.

**REST Proxy placement.** Co-locate with the Kafka cluster in the same
AZ/datacenter. The Proxy makes many native-protocol calls per HTTP
request; cross-region latency multiplies.

**Disk.** REST Proxy does not write to disk in normal operation, only
logs.

**Authorization.** Enforce at the broker, not at REST Proxy. ACLs in
Kafka are the source of truth; the Proxy is a transport.

## Summary

- The Admin API is the programmatic interface to all Kafka administration:
  topics, ACLs, configs, consumer groups, quotas.
- It is asynchronous (`KafkaFuture`), batch-oriented, and has two
  overloads per method (short form + `*Options`).
- In modern deployments the Admin API is almost always called *via* a
  declarative layer (Strimzi, Terraform) or a UI, not directly from
  hand-written application code.
- REST Proxy is the HTTP face of Kafka for clients that cannot use a
  native Kafka client library. It is a transport; ACLs at the broker
  remain the source of truth.

The lab (`LAB.md`) demonstrates the declarative GitOps approach using
Terraform against the lesson 6 broker on AWS.
