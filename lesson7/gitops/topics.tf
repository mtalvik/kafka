# Topics managed by Terraform.
#
# - orders, payments, user-profiles: lesson 7 GitOps demo topics
# - logs: hw2 pipeline topic (Filebeat → Kafka → Vector → OpenSearch)

resource "kafka_topic" "orders" {
  name               = "orders"
  partitions         = 3
  replication_factor = 1

  config = {
    "cleanup.policy"   = "delete"
    "retention.ms"     = "604800000"
    "compression.type" = "producer"
  }
}

resource "kafka_topic" "payments" {
  name               = "payments"
  partitions         = 3
  replication_factor = 1

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "604800000"
  }
}

resource "kafka_topic" "user_profiles" {
  name               = "user-profiles"
  partitions         = 1
  replication_factor = 1

  config = {
    "cleanup.policy" = "compact"
    "segment.ms"     = "60000"
  }
}

# hw2 pipeline topic. Owned by Terraform from lesson 7 onward.
resource "kafka_topic" "logs" {
  name               = "logs"
  partitions         = 3
  replication_factor = 1

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "86400000" # 1 day — log data, short retention
  }

  lifecycle {
    prevent_destroy = true
  }
}
