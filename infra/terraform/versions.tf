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
  }

  # Descomente para armazenar o state remotamente (GCS recomendado para equipe):
  # backend "gcs" {
  #   bucket = "pulso-tf-state-<project_id>"
  #   prefix = "pulso/state"
  # }
}
