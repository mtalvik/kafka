output "topics" {
  description = "Topics managed by Terraform."
  value = [
    kafka_topic.orders.name,
    kafka_topic.payments.name,
    kafka_topic.user_profiles.name,
    kafka_topic.logs.name,
    kafka_topic.producer_lab.name,
  ]
}

output "acls" {
  description = "ACL summary per principal."
  value = {
    "User:alice" = "Write/Describe on orders, logs (hw2 producer), producer-lab (lesson 8)"
    "User:bob"   = "Read/Describe on orders, logs (hw2 consumer), producer-lab (lesson 8); Read on any group"
    "User:charlie" = "no ACLs — denied by default (negative test principal)"
  }
}
