# ─────────────────────────────────────────────────────────────
# Service Accounts — um por serviço Cloud Run (princípio do
# menor privilégio: cada SA recebe só o que precisa).
# ─────────────────────────────────────────────────────────────

resource "google_service_account" "pulso_ingest" {
  account_id   = "pulso-ingest-sa"
  display_name = "Pulso Ingest (producer WebSocket)"
}

resource "google_service_account" "pulso_sink" {
  account_id   = "pulso-sink-sa"
  display_name = "Pulso Sink (Kafka → Iceberg)"
}

resource "google_service_account" "pulso_anomaly" {
  account_id   = "pulso-anomaly-sa"
  display_name = "Pulso Anomaly Detector"
}

resource "google_service_account" "pulso_llm_explainer" {
  account_id   = "pulso-llm-explainer-sa"
  display_name = "Pulso LLM Explainer"
}

resource "google_service_account" "pulso_serve" {
  account_id   = "pulso-serve-sa"
  display_name = "Pulso Serve (FastAPI)"
}

# SA do deploy CI/CD (GitHub Actions via Workload Identity Federation)
resource "google_service_account" "pulso_deploy" {
  account_id   = "pulso-deploy-sa"
  display_name = "Pulso CI/CD deploy (GitHub Actions)"
}

# ─────────────────────────────────────────────────────────────
# GCS — lake bucket
# ─────────────────────────────────────────────────────────────

# Sink escreve no lake (objectAdmin)
resource "google_storage_bucket_iam_member" "sink_lake_admin" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pulso_sink.email}"
}

# Serve lê do lake (objectViewer)
resource "google_storage_bucket_iam_member" "serve_lake_viewer" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.pulso_serve.email}"
}

# ─────────────────────────────────────────────────────────────
# GCS — anomaly DB bucket (leitura+escrita pelo explainer e serve)
# ─────────────────────────────────────────────────────────────

resource "google_storage_bucket_iam_member" "explainer_anomaly_admin" {
  bucket = google_storage_bucket.anomaly_db.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pulso_llm_explainer.email}"
}

resource "google_storage_bucket_iam_member" "serve_anomaly_viewer" {
  bucket = google_storage_bucket.anomaly_db.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.pulso_serve.email}"
}

# ─────────────────────────────────────────────────────────────
# Secret Manager — cada SA acessa apenas seus próprios secrets
# ─────────────────────────────────────────────────────────────

locals {
  # Quais SAs precisam de acesso a quais secrets
  secret_accessors = {
    "ingest-kafka-bootstrap"    = { sa = google_service_account.pulso_ingest.email, secret = google_secret_manager_secret.kafka_bootstrap.secret_id }
    "ingest-schema-registry"    = { sa = google_service_account.pulso_ingest.email, secret = google_secret_manager_secret.schema_registry_url.secret_id }
    "sink-kafka-bootstrap"      = { sa = google_service_account.pulso_sink.email, secret = google_secret_manager_secret.kafka_bootstrap.secret_id }
    "sink-schema-registry"      = { sa = google_service_account.pulso_sink.email, secret = google_secret_manager_secret.schema_registry_url.secret_id }
    "sink-catalog-uri"          = { sa = google_service_account.pulso_sink.email, secret = google_secret_manager_secret.iceberg_catalog_uri.secret_id }
    "sink-warehouse"            = { sa = google_service_account.pulso_sink.email, secret = google_secret_manager_secret.iceberg_warehouse.secret_id }
    "anomaly-kafka-bootstrap"   = { sa = google_service_account.pulso_anomaly.email, secret = google_secret_manager_secret.kafka_bootstrap.secret_id }
    "anomaly-schema-registry"   = { sa = google_service_account.pulso_anomaly.email, secret = google_secret_manager_secret.schema_registry_url.secret_id }
    "explainer-kafka-bootstrap" = { sa = google_service_account.pulso_llm_explainer.email, secret = google_secret_manager_secret.kafka_bootstrap.secret_id }
    "explainer-schema-registry" = { sa = google_service_account.pulso_llm_explainer.email, secret = google_secret_manager_secret.schema_registry_url.secret_id }
    "explainer-anthropic"       = { sa = google_service_account.pulso_llm_explainer.email, secret = google_secret_manager_secret.anthropic_api_key.secret_id }
    "serve-catalog-uri"         = { sa = google_service_account.pulso_serve.email, secret = google_secret_manager_secret.iceberg_catalog_uri.secret_id }
    "serve-warehouse"           = { sa = google_service_account.pulso_serve.email, secret = google_secret_manager_secret.iceberg_warehouse.secret_id }
    "serve-kafka-bootstrap"     = { sa = google_service_account.pulso_serve.email, secret = google_secret_manager_secret.kafka_bootstrap.secret_id }
    "serve-schema-registry"     = { sa = google_service_account.pulso_serve.email, secret = google_secret_manager_secret.schema_registry_url.secret_id }
    "serve-ksqldb-url"          = { sa = google_service_account.pulso_serve.email, secret = google_secret_manager_secret.ksqldb_url.secret_id }
  }
}

resource "google_secret_manager_secret_iam_member" "secret_accessors" {
  for_each  = local.secret_accessors
  secret_id = each.value.secret
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${each.value.sa}"
}

# ─────────────────────────────────────────────────────────────
# Monitoring — o sidecar GMP de cada serviço escreve métricas no
# Managed Service for Prometheus (roles/monitoring.metricWriter).
# ─────────────────────────────────────────────────────────────

resource "google_project_iam_member" "metric_writers" {
  for_each = toset([
    google_service_account.pulso_ingest.email,
    google_service_account.pulso_sink.email,
    google_service_account.pulso_anomaly.email,
    google_service_account.pulso_llm_explainer.email,
    google_service_account.pulso_serve.email,
  ])
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${each.value}"
}

# ─────────────────────────────────────────────────────────────
# Segurança do barramento (item 5) — acesso aos secrets de TLS/SASL.
# ─────────────────────────────────────────────────────────────

# Todos os 5 serviços Cloud Run precisam do CA cert e da senha SASL.
resource "google_secret_manager_secret_iam_member" "bus_security_accessors" {
  for_each = {
    for pair in setproduct(
      [
        google_service_account.pulso_ingest.email,
        google_service_account.pulso_sink.email,
        google_service_account.pulso_anomaly.email,
        google_service_account.pulso_llm_explainer.email,
        google_service_account.pulso_serve.email,
      ],
      [
        google_secret_manager_secret.tls_ca.secret_id,
        google_secret_manager_secret.kafka_sasl_password.secret_id,
      ]
    ) : "${pair[0]}|${pair[1]}" => { sa = pair[0], secret = pair[1] }
  }
  secret_id = each.value.secret
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${each.value.sa}"
}

# A VM do broker lê cert, chave, CA e a senha SASL (para criar o usuário SCRAM).
resource "google_secret_manager_secret_iam_member" "redpanda_secret_accessors" {
  for_each = toset([
    google_secret_manager_secret.redpanda_cert.secret_id,
    google_secret_manager_secret.redpanda_key.secret_id,
    google_secret_manager_secret.tls_ca.secret_id,
    google_secret_manager_secret.kafka_sasl_password.secret_id,
  ])
  secret_id = each.value
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.redpanda.email}"
}

# ─────────────────────────────────────────────────────────────
# Cloud SQL — sink e serve precisam acessar o catálogo Iceberg
# ─────────────────────────────────────────────────────────────

resource "google_project_iam_member" "sink_cloudsql" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.pulso_sink.email}"
}

resource "google_project_iam_member" "serve_cloudsql" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.pulso_serve.email}"
}

# ─────────────────────────────────────────────────────────────
# Deploy SA — push de imagens + update Cloud Run
# ─────────────────────────────────────────────────────────────

# Artifact Registry reader — todos os serviços Cloud Run precisam puxar a imagem
resource "google_artifact_registry_repository_iam_member" "cloudrun_sa_readers" {
  for_each   = toset([
    google_service_account.pulso_ingest.email,
    google_service_account.pulso_sink.email,
    google_service_account.pulso_anomaly.email,
    google_service_account.pulso_llm_explainer.email,
    google_service_account.pulso_serve.email,
  ])
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.pulso.repository_id
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${each.value}"
}

resource "google_project_iam_member" "deploy_run_admin" {
  project = var.project_id
  role    = "roles/run.admin"
  member  = "serviceAccount:${google_service_account.pulso_deploy.email}"
}

resource "google_project_iam_member" "deploy_ar_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${google_service_account.pulso_deploy.email}"
}

resource "google_project_iam_member" "deploy_sa_user" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.pulso_deploy.email}"
}

# ─────────────────────────────────────────────────────────────
# Workload Identity Federation — GitHub Actions sem chave longa
# ─────────────────────────────────────────────────────────────

resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-actions-pool"
  display_name              = "GitHub Actions"
  description               = "Pool para CI/CD do Pulso via GitHub Actions OIDC"
  disabled                  = false

  depends_on = [google_project_service.apis]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-provider"
  display_name                       = "GitHub OIDC"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.actor"      = "assertion.actor"
    "attribute.repository" = "assertion.repository"
  }

  # Slug real do repositório GitHub: "owner/repo"
  attribute_condition = "attribute.repository == \"GscDtAnalytic/Pulso\""

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account_iam_binding" "deploy_wif" {
  service_account_id = google_service_account.pulso_deploy.name
  role               = "roles/iam.workloadIdentityUser"

  members = [
    "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/GscDtAnalytic/Pulso",
  ]
}
