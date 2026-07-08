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
