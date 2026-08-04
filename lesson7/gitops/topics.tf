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

# ---------------------------------------------------------------------------
# lesson 14 Kafka Streams DSL lab topics.
#
# Two partitions on the inputs so the repartition step in the aggregation
# and join exercises is observable rather than a no-op. companies and
# customers back GlobalKTables: compacted, because a table is a
# latest-value-per-key view and must survive retention.
# ---------------------------------------------------------------------------

resource "kafka_topic" "streams_dsl_lab" {
  for_each = toset([
    "stock-ticks",
    "stock-transactions",
    "top-shares",
    "windowed-counts",
    "transaction-summary",
    "enriched-summary",
  ])

  name               = each.key
  partitions         = 2
  replication_factor = 1

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "3600000" # 1 hour - lab data, throwaway
  }
}

resource "kafka_topic" "streams_dsl_lookup" {
  for_each = toset(["companies", "customers"])

  name               = each.key
  partitions         = 2
  replication_factor = 1

  config = {
    "cleanup.policy" = "compact"
    "segment.ms"     = "60000"
  }
}

# ---------------------------------------------------------------------------
# lesson 15 Kafka Streams lab topics.
#
#   src-topic / out-topic          : Ex1 upper-case topology
#   purchases                      : shared input for Ex2-Ex4. TWO partitions
#                                    on purpose - Ex4 produces a correct
#                                    per-customer total only because it
#                                    repartitions by customerId first.
#   purchases-masked / rewards /
#   patterns / expensive-purchases /
#   cafe-sales / electronics-sales : Ex2 and Ex3 sinks
#   electronics-events /
#   cafe-events / coupons          : Ex5 windowed join
#   events                         : Ex6 session-window homework
#
# Streams also creates internal topics at runtime (changelog, repartition),
# prefixed with the application.id. Those are NOT declared here - the app
# creates them, which is why the principal needs Create on that prefix in
# acls.tf.
# ---------------------------------------------------------------------------

resource "kafka_topic" "streams_lab" {
  for_each = {
    "src-topic"           = 1
    "out-topic"           = 1
    "purchases"           = 2
    "purchases-masked"    = 1
    "rewards"             = 1
    "patterns"            = 1
    "expensive-purchases" = 1
    "cafe-sales"          = 1
    "electronics-sales"   = 1
    "electronics-events"  = 1
    "cafe-events"         = 1
    "coupons"             = 1
    # 3 partitions: hw4 feeds several keys by hand and the point is that they
    # hash to different partitions, so the app does real per-key work rather
    # than reading one ordered log. lesson15 Ex6 is unaffected.
    "events"              = 3
  }

  name               = each.key
  partitions         = each.value
  replication_factor = 1

  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "3600000" # 1 hour - lab data, throwaway
  }
}
