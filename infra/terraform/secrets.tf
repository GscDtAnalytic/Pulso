# Os recursos google_secret_manager_secret criam o container do secret.
# Os valores (versions) são populados fora do Terraform:
#   gcloud secrets versions add pulso-kafka-bootstrap --data-file=-  <<< "seed-xxx:9092"
#
# Exceção: iceberg_catalog_uri e db_password são geridos aqui mesmo.

resource "google_secret_manager_secret" "kafka_bootstrap" {
  secret_id = "pulso-kafka-bootstrap"
  replication { auto {} }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret" "schema_registry_url" {
  secret_id = "pulso-schema-registry-url"
  replication { auto {} }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret" "anthropic_api_key" {
  secret_id = "pulso-anthropic-api-key"
  replication { auto {} }
  depends_on = [google_project_service.apis]
}

# Gerado pelo Terraform; URI completo com host Cloud SQL unix socket.
resource "google_secret_manager_secret" "iceberg_catalog_uri" {
  secret_id = "pulso-iceberg-catalog-uri"
  replication { auto {} }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "iceberg_catalog_uri" {
  secret = google_secret_manager_secret.iceberg_catalog_uri.id
  # Conexão via Cloud SQL Auth Proxy (unix socket montado no container Cloud Run).
  secret_data = "postgresql+psycopg2://pulso:${random_password.db_password.result}@/pulso?host=/cloudsql/${local.cloud_sql_instance}"
}

resource "google_secret_manager_secret" "iceberg_warehouse" {
  secret_id = "pulso-iceberg-warehouse"
  replication { auto {} }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "iceberg_warehouse" {
  secret      = google_secret_manager_secret.iceberg_warehouse.id
  secret_data = "gs://${google_storage_bucket.lake.name}/warehouse"
}

# Seed dos valores variáveis (kafka_bootstrap, schema_registry_url, anthropic_api_key)
# se as variáveis forem fornecidas no tfvars. Caso contrário, populate manualmente.
resource "google_secret_manager_secret_version" "kafka_bootstrap" {
  count       = var.kafka_bootstrap != "" ? 1 : 0
  secret      = google_secret_manager_secret.kafka_bootstrap.id
  secret_data = var.kafka_bootstrap
}

resource "google_secret_manager_secret_version" "schema_registry_url" {
  count       = var.schema_registry_url != "" ? 1 : 0
  secret      = google_secret_manager_secret.schema_registry_url.id
  secret_data = var.schema_registry_url
}

resource "google_secret_manager_secret_version" "anthropic_api_key" {
  count       = var.anthropic_api_key != "" ? 1 : 0
  secret      = google_secret_manager_secret.anthropic_api_key.id
  secret_data = var.anthropic_api_key
}
