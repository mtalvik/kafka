variable "bootstrap_servers" {
  description = "Kafka broker bootstrap server endpoints, e.g. [\"broker.internal:9092\"]."
  type        = list(string)
}

variable "admin_username" {
  description = "SASL/PLAIN username with super-user rights on the broker."
  type        = string
}

variable "admin_password" {
  description = "SASL/PLAIN password for admin_username. Provide via TF_VAR_admin_password or terraform.tfvars (never commit)."
  type        = string
  sensitive   = true
}
