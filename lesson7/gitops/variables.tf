variable "bootstrap_servers" {
  description = "Kafka broker bootstrap server (host:port). Defaults to the hw2 EC2 private IP."
  type        = list(string)
  default     = ["172.31.29.117:9092"]
}

variable "admin_username" {
  description = "SASL/PLAIN username with super-user rights on the broker."
  type        = string
  default     = "admin"
}

variable "admin_password" {
  description = "SASL/PLAIN password for admin_username. Provide via TF_VAR_admin_password or a .tfvars file (never commit)."
  type        = string
  sensitive   = true
}
