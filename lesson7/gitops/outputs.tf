output "topics" {
  description = "Topics managed by Terraform."
  value = [
    kafka_topic.orders.name,
    kafka_topic.payments.name,
    kafka_topic.user_profiles.name,
  ]
}

output "acls" {
  description = "ACL principal → operation → resource mapping managed by Terraform."
  value = {
    "User:alice" = "Write/Describe on topic '${kafka_topic.orders.name}'"
    "User:bob"   = "Read/Describe on topic '${kafka_topic.orders.name}', Read on any consumer group"
  }
}
