output "topics" {
  description = "Topics managed by Terraform."
  value = [
    kafka_topic.orders.name,
    kafka_topic.payments.name,
    kafka_topic.user_profiles.name,
    kafka_topic.logs.name,
    kafka_topic.producer_lab.name,
    kafka_topic.tx_a.name,
    kafka_topic.tx_b.name,
    kafka_topic.tx_inbound.name,
    kafka_topic.tx_outbound.name,
  ]
}

output "acls" {
  description = "ACL summary per principal."
  value = {
    "User:alice" = "Write/Describe on orders, logs (hw2 producer), producer-lab (lesson 8); tx- prefixed: Write/Read/Describe on topics, Write/Describe on TransactionalID, Read on groups (lesson 10)"
    "User:bob"   = "Read/Describe on orders, logs (hw2 consumer), producer-lab (lesson 8), tx- topics (lesson 10); Read on any group"
    "User:charlie" = "no ACLs — denied by default (negative test principal)"
  }
}
