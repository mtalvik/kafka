terraform {
  required_version = ">= 1.5"

  required_providers {
    kafka = {
      source  = "Mongey/kafka"
      version = "~> 0.8"
    }
  }
}
