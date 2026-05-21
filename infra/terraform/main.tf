provider "google" {
  project = var.project_id
  region  = var.region
}

# APIs necessárias
resource "google_project_service" "apis" {
  for_each = toset([
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "sqladmin.googleapis.com",
    "secretmanager.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",  # Workload Identity Federation (CI/CD)
  ])

  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

locals {
  image = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.pulso.repository_id}/pulso:${var.image_tag}"

  # Env vars comuns a todos os serviços
  common_env = {
    PULSO_ENV      = "prod"
    PULSO_LOG_JSON = "true"
  }

  # Secrets comuns: Kafka + Schema Registry + catálogo Iceberg
  common_secrets = {
    PULSO_KAFKA_BOOTSTRAP      = google_secret_manager_secret.kafka_bootstrap.secret_id
    PULSO_SCHEMA_REGISTRY_URL  = google_secret_manager_secret.schema_registry_url.secret_id
    PULSO_ICEBERG_CATALOG_URI  = google_secret_manager_secret.iceberg_catalog_uri.secret_id
    PULSO_ICEBERG_WAREHOUSE    = google_secret_manager_secret.iceberg_warehouse.secret_id
  }

  # Cloud SQL connection name para montagem do proxy unix socket
  cloud_sql_instance = google_sql_database_instance.iceberg_catalog.connection_name
}

# Senha do banco gerada automaticamente e armazenada no Secret Manager
resource "random_password" "db_password" {
  length  = 24
  special = false
}
