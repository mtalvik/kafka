output "topics" {
  description = "Topics managed by Terraform."
  value = [
    kafka_topic.orders.name,
    kafka_topic.payments.name,
    kafka_topic.user_profiles.name,
    kafka_topic.logs.name,
  ]
}

output "acls" {
  description = "ACL summary per principal."
  value = {
    "User:alice" = "Write/Describe on orders, Write/Describe on logs (hw2 producer)"
    "User:bob"   = "Read/Describe on orders, Read/Describe on logs (hw2 consumer), Read on any group"
    "User:charlie" = "no ACLs — denied by default (negative test principal)"
  }
}
