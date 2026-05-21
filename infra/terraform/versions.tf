terraform {
  required_version = ">= 1.8"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }

  # State remoto no GCS — obrigatório: o state contém segredos
  # (random_password do banco). Config parcial: bucket/prefix vêm de
  # backend.hcl via `terraform init -backend-config=backend.hcl`.
  # Bootstrap do bucket: ver RUNBOOK_PROD.md §4.
  backend "gcs" {}
}
