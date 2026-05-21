output "lake_bucket" {
  description = "Nome do bucket GCS do lakehouse Iceberg."
  value       = google_storage_bucket.lake.name
}

output "anomaly_db_bucket" {
  description = "Nome do bucket GCS que armazena o DuckDB de anomalias."
  value       = google_storage_bucket.anomaly_db.name
}

output "artifact_registry_url" {
  description = "URL base do Artifact Registry para push/pull de imagens."
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.pulso.repository_id}"
}

output "cloud_sql_connection_name" {
  description = "Connection name do Cloud SQL (project:region:instance) para uso no proxy."
  value       = google_sql_database_instance.iceberg_catalog.connection_name
}

output "serve_url" {
  description = "URL pública do serviço pulso-serve (FastAPI)."
  value       = google_cloud_run_v2_service.pulso_serve.uri
}

output "wif_provider" {
  description = "Resource name do Workload Identity Provider (usar em GOOGLE_OIDC_TOKEN_REQUEST_URL do GitHub Actions)."
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "deploy_sa_email" {
  description = "E-mail da service account de deploy (GitHub Actions)."
  value       = google_service_account.pulso_deploy.email
}
