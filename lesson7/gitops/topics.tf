# Topics managed by Terraform.
#
# - orders, payments, user-profiles: lesson 7 GitOps demo topics
# - logs: hw2 pipeline topic (Filebeat → Kafka → Vector → OpenSearch)
# - producer-lab: lesson 8 Producer API examples (Ex1–Ex5)

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

# lesson 8 Producer API lab topic. 3 partitions so key-based partitioning
# (Ex4) and sticky partitioner behavior (Ex1) are observable.
resource "kafka_topic" "producer_lab" {
  name               = "producer-lab"
  partitions         = 3
  replication_factor = 1

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "3600000" # 1 hour — lab data, throwaway
  }
}

# ---------------------------------------------------------------------------
# lesson 10 Transactions / Exactly Once lab topics.
#   tx-a, tx-b : Ex5 atomic multi-topic write; Ex7/Ex8 reuse tx-a
#   tx-inbound, tx-outbound : Ex6 read-process-write (EOS loop)
# Single partition each — visibility and ordering are the point here,
# not parallelism.
# ---------------------------------------------------------------------------

resource "kafka_topic" "tx_a" {
  name               = "tx-a"
  partitions         = 1
  replication_factor = 1

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "3600000"
  }
}

resource "kafka_topic" "tx_b" {
  name               = "tx-b"
  partitions         = 1
  replication_factor = 1

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "3600000"
  }
}

resource "kafka_topic" "tx_inbound" {
  name               = "tx-inbound"
  partitions         = 1
  replication_factor = 1

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "3600000"
  }
}

resource "kafka_topic" "tx_outbound" {
  name               = "tx-outbound"
  partitions         = 1
  replication_factor = 1

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "3600000"
  }
}
