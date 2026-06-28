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
