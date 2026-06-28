resource "kafka_acl" "alice_write_orders" {
  resource_name                = kafka_topic.orders.name
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal      = "User:alice"
  acl_host           = "*"
  acl_operation      = "Write"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "alice_describe_orders" {
  resource_name                = kafka_topic.orders.name
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal      = "User:alice"
  acl_host           = "*"
  acl_operation      = "Describe"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "bob_read_orders" {
  resource_name                = kafka_topic.orders.name
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal      = "User:bob"
  acl_host           = "*"
  acl_operation      = "Read"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "bob_describe_orders" {
  resource_name                = kafka_topic.orders.name
  resource_type                = "Topic"
  resource_pattern_type_filter = "Literal"

  acl_principal      = "User:bob"
  acl_host           = "*"
  acl_operation      = "Describe"
  acl_permission_type = "Allow"
}

resource "kafka_acl" "bob_read_any_group" {
  resource_name                = "*"
  resource_type                = "Group"
  resource_pattern_type_filter = "Literal"

  acl_principal      = "User:bob"
  acl_host           = "*"
  acl_operation      = "Read"
  acl_permission_type = "Allow"
}
