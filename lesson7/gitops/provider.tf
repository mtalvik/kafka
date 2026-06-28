provider "kafka" {
  bootstrap_servers = var.bootstrap_servers

  sasl_username = var.admin_username
  sasl_password = var.admin_password
  sasl_mechanism = "plain"

  tls_enabled = false
}
