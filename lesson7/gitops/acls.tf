# ACLs managed by Terraform.
#
# - alice/bob ACLs on "orders": lesson 7 GitOps demo
# - alice/bob ACLs on "logs": hw2 pipeline
#     - alice: Filebeat producer (WRITE + DESCRIBE)
#     - bob: Vector consumer (READ + DESCRIBE on topic, READ on any group)
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
