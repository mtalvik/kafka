# ACLs managed by Terraform.
#
# - alice/bob ACLs on "orders": lesson 7 GitOps demo
# - alice/bob ACLs on "logs": hw2 pipeline
#     - alice: Filebeat producer (WRITE + DESCRIBE)
#     - bob: Vector consumer (READ + DESCRIBE on topic, READ on any group)
# - alice/bob ACLs on "producer-lab": lesson 8 Java producer + verification consumer
# - charlie: no ACLs — must remain denied for negative tests

# ---------------------------------------------------------------------------
# Lesson 7 demo — orders topic
# ---------------------------------------------------------------------------

resource "kafka_acl" "alice_write_orders" {
  resource_name                = kafka_topic.orders.name
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal       = "User:alice"
  acl_host            = "*"
  acl_operation       = "Write"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "alice_describe_orders" {
  resource_name                = kafka_topic.orders.name
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal       = "User:alice"
  acl_host            = "*"
  acl_operation       = "Describe"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "bob_read_orders" {
  resource_name                = kafka_topic.orders.name
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal       = "User:bob"
  acl_host            = "*"
  acl_operation       = "Read"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "bob_describe_orders" {
  resource_name                = kafka_topic.orders.name
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal       = "User:bob"
  acl_host            = "*"
  acl_operation       = "Describe"
  acl_permission_type = "Allow"
}

# ---------------------------------------------------------------------------
# hw2 pipeline — logs topic
# ---------------------------------------------------------------------------

resource "kafka_acl" "alice_write_logs" {
  resource_name                = kafka_topic.logs.name
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal       = "User:alice"
  acl_host            = "*"
  acl_operation       = "Write"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "alice_describe_logs" {
  resource_name                = kafka_topic.logs.name
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal       = "User:alice"
  acl_host            = "*"
  acl_operation       = "Describe"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "bob_read_logs" {
  resource_name                = kafka_topic.logs.name
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal       = "User:bob"
  acl_host            = "*"
  acl_operation       = "Read"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "bob_describe_logs" {
  resource_name                = kafka_topic.logs.name
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal       = "User:bob"
  acl_host            = "*"
  acl_operation       = "Describe"
  acl_permission_type = "Allow"
}

# ---------------------------------------------------------------------------
# Shared — consumer groups
# ---------------------------------------------------------------------------

resource "kafka_acl" "bob_read_any_group" {
  resource_name                = "*"
  resource_type                = "Group"
  resource_pattern_type_filter = "Literal"

  acl_principal       = "User:bob"
  acl_host            = "*"
  acl_operation       = "Read"
  acl_permission_type = "Allow"
}

# ---------------------------------------------------------------------------
# Lesson 8 lab — producer-lab topic
# ---------------------------------------------------------------------------

resource "kafka_acl" "alice_write_producer_lab" {
  resource_name                = kafka_topic.producer_lab.name
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal       = "User:alice"
  acl_host            = "*"
  acl_operation       = "Write"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "alice_describe_producer_lab" {
  resource_name                = kafka_topic.producer_lab.name
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal       = "User:alice"
  acl_host            = "*"
  acl_operation       = "Describe"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "bob_read_producer_lab" {
  resource_name                = kafka_topic.producer_lab.name
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal       = "User:bob"
  acl_host            = "*"
  acl_operation       = "Read"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "bob_describe_producer_lab" {
  resource_name                = kafka_topic.producer_lab.name
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal       = "User:bob"
  acl_host            = "*"
  acl_operation       = "Describe"
  acl_permission_type = "Allow"
}

# ---------------------------------------------------------------------------
# Lesson 10 lab — Transactions / Exactly Once
#
#   alice: transactional producer + Ex6 transformer (consume tx-inbound,
#          produce transactionally to tx-*). Needs, beyond plain WRITE:
#            - a TransactionalId ACL (new resource type this lesson)
#            - READ on the source topic and on the transformer group
#   bob:   read_committed verification consumer on tx-a / tx-outbound
#          (already has READ on any group from the shared section above)
#
# transactional.id values used by the examples are tx-ex5..tx-ex8, so a
# single PREFIXED "tx-" ACL covers all of them instead of one per id.
# ---------------------------------------------------------------------------

# --- alice: TransactionalId (prefixed) ---

resource "kafka_acl" "alice_write_txid" {
  resource_name                = "tx-"
  resource_type                = "TransactionalID"
  resource_pattern_type_filter = "Prefixed"

  acl_principal       = "User:alice"
  acl_host            = "*"
  acl_operation       = "Write"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "alice_describe_txid" {
  resource_name                = "tx-"
  resource_type                = "TransactionalID"
  resource_pattern_type_filter = "Prefixed"

  acl_principal       = "User:alice"
  acl_host            = "*"
  acl_operation       = "Describe"
  acl_permission_type = "Allow"
}

# --- alice: Read on the Ex6 transformer group (prefixed) ---

resource "kafka_acl" "alice_read_tx_group" {
  resource_name                = "tx-"
  resource_type                = "Group"
  resource_pattern_type_filter = "Prefixed"

  acl_principal       = "User:alice"
  acl_host            = "*"
  acl_operation       = "Read"
  acl_permission_type = "Allow"
}

# --- alice: Write/Read/Describe on tx topics (prefixed) ---
# Read is needed because the Ex6 transformer also consumes tx-inbound.

resource "kafka_acl" "alice_write_tx_topics" {
  resource_name                = "tx-"
  resource_type                = "Topic"
  resource_pattern_type_filter = "Prefixed"

  acl_principal       = "User:alice"
  acl_host            = "*"
  acl_operation       = "Write"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "alice_read_tx_topics" {
  resource_name                = "tx-"
  resource_type                = "Topic"
  resource_pattern_type_filter = "Prefixed"

  acl_principal       = "User:alice"
  acl_host            = "*"
  acl_operation       = "Read"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "alice_describe_tx_topics" {
  resource_name                = "tx-"
  resource_type                = "Topic"
  resource_pattern_type_filter = "Prefixed"

  acl_principal       = "User:alice"
  acl_host            = "*"
  acl_operation       = "Describe"
  acl_permission_type = "Allow"
}

# --- bob: read_committed verifier on tx topics (prefixed) ---

resource "kafka_acl" "bob_read_tx_topics" {
  resource_name                = "tx-"
  resource_type                = "Topic"
  resource_pattern_type_filter = "Prefixed"

  acl_principal       = "User:bob"
  acl_host            = "*"
  acl_operation       = "Read"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "bob_describe_tx_topics" {
  resource_name                = "tx-"
  resource_type                = "Topic"
  resource_pattern_type_filter = "Prefixed"

  acl_principal       = "User:bob"
  acl_host            = "*"
  acl_operation       = "Describe"
  acl_permission_type = "Allow"
}

# ---------------------------------------------------------------------------
# Lessons 14 and 15 - Kafka Streams
#
# Both lessons run their topologies as bob. A Streams app is a consumer, a
# producer and an admin client at once, so the principal needs more than the
# usual read-or-write split:
#
#   - Read + Write + Describe on every source and sink topic. Which is which
#     differs per exercise, so granting all three on the lab topics keeps this
#     block readable instead of tracking direction topic by topic.
#   - Read + Write + Describe + Create + Delete on the PREFIXED internal
#     topics. Streams creates changelog and repartition topics itself at
#     startup, named "<application.id>-...", hence Create. Delete is for
#     kafka-streams-application-reset between runs.
#   - Read on the consumer Group named after application.id. bob already has
#     Read on Group "*" in the shared section above, so nothing is added here.
#
# application.id convention: lesson14-ex1..ex4, lesson15-ex1..ex6,
# lesson16-ex1..ex5. The three prefixes below cover every internal topic of
# all three lessons.
# ---------------------------------------------------------------------------

locals {
  streams_lab_topics = concat(
    keys(kafka_topic.streams_dsl_lab),
    keys(kafka_topic.streams_dsl_lookup),
    keys(kafka_topic.streams_lab),
    keys(kafka_topic.streams_papi_lab),
  )

  streams_topic_grants = {
    for pair in setproduct(local.streams_lab_topics, ["Read", "Write", "Describe"]) :
    "${pair[0]}-${lower(pair[1])}" => {
      topic     = pair[0]
      operation = pair[1]
    }
  }

  streams_internal_grants = {
    for pair in setproduct(["lesson14-", "lesson15-", "lesson16-"], ["Read", "Write", "Describe", "Create", "Delete"]) :
    "${pair[0]}${lower(pair[1])}" => {
      prefix    = pair[0]
      operation = pair[1]
    }
  }
}

resource "kafka_acl" "bob_streams_lab_topics" {
  for_each = local.streams_topic_grants

  resource_name                = each.value.topic
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal       = "User:bob"
  acl_host            = "*"
  acl_operation       = each.value.operation
  acl_permission_type = "Allow"
}

resource "kafka_acl" "bob_streams_internal_topics" {
  for_each = local.streams_internal_grants

  resource_name                = each.value.prefix
  resource_type                = "Topic"
  resource_pattern_type_filter = "Prefixed"

  acl_principal       = "User:bob"
  acl_host            = "*"
  acl_operation       = each.value.operation
  acl_permission_type = "Allow"
}

# ---------------------------------------------------------------------------
# Lesson 16 - exactly-once in Kafka Streams
#
# Setting processing.guarantee = exactly_once_v2 turns the Streams producer
# into a transactional producer, so bob needs the same TransactionalID grants
# alice got in lesson 10 - but on a different prefix.
#
# Streams does not let you choose the transactional.id: it derives it from
# application.id ("<application.id>-<processId>-<threadIdx>" under v2). Since
# every lesson 16 application.id starts with "lesson16-", one PREFIXED ACL
# covers all five exercises.
#
# This is exactly why the application.id must be stable. An id built as
# "app-" + UUID.randomUUID() produces a different transactional.id on every
# run, and no reasonable prefix ACL can cover it - the run fails with
# TransactionalIdAuthorizationException. Worth showing students on purpose.
# ---------------------------------------------------------------------------

resource "kafka_acl" "bob_write_lesson16_txid" {
  resource_name                = "lesson16-"
  resource_type                = "TransactionalID"
  resource_pattern_type_filter = "Prefixed"

  acl_principal       = "User:bob"
  acl_host            = "*"
  acl_operation       = "Write"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "bob_describe_lesson16_txid" {
  resource_name                = "lesson16-"
  resource_type                = "TransactionalID"
  resource_pattern_type_filter = "Prefixed"

  acl_principal       = "User:bob"
  acl_host            = "*"
  acl_operation       = "Describe"
  acl_permission_type = "Allow"
}

# IdempotentWrite on the cluster.
#
# On brokers from 3.0 onward this is not strictly required - Write on a topic
# is enough to produce idempotently, and the broker no longer checks
# IdempotentWrite separately. Granted anyway: it is harmless, it matches what
# every transactional-producer reference still lists, and it removes one
# variable if EOS initialisation misbehaves during the lab.
#
# The cluster resource is always named "kafka-cluster" in the ACL API.

resource "kafka_acl" "bob_idempotent_write" {
  resource_name                = "kafka-cluster"
  resource_type                = "Cluster"
  resource_pattern_type_filter = "Literal"

  acl_principal       = "User:bob"
  acl_host            = "*"
  acl_operation       = "IdempotentWrite"
  acl_permission_type = "Allow"
}
