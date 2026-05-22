# ─────────────────────────────────────────────────────────────
# Espelho DuckDB do lake — Cloud Run Job + Cloud Scheduler
#
# Roda a cada hora (offset 30 min em relação ao Soda):
#   1. `python -m pulso_storage mirror`  — Iceberg GCS → DuckDB
#   2. `dbt build --target dev`          — materializa fct_candle/fct_symbol_daily
#
# O arquivo `pulso_lake.duckdb` é gravado diretamente no bucket
# via volume GCS montado no container. O pulso-serve monta o
# mesmo bucket em read-only: histórico sempre consistente com o
# último job bem-sucedido.
#
# Princípio #4: fail-loud (max_retries=0).
# ─────────────────────────────────────────────────────────────

# ── Bucket ───────────────────────────────────────────────────
resource "google_storage_bucket" "lake_mirror" {
  name          = "pulso-lake-mirror-${var.project_id}"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  depends_on = [google_project_service.apis]
}

# ── Service Account ───────────────────────────────────────────
resource "google_service_account" "pulso_lake_mirror" {
  account_id   = "pulso-lake-mirror-sa"
  display_name = "Pulso Lake Mirror Job"
}

# ── IAM — job ─────────────────────────────────────────────────

# Lê tabelas Iceberg do lake principal
resource "google_storage_bucket_iam_member" "lake_mirror_lake_viewer" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.pulso_lake_mirror.email}"
}

# Escreve o DuckDB no bucket de espelho
resource "google_storage_bucket_iam_member" "lake_mirror_bucket_admin" {
  bucket = google_storage_bucket.lake_mirror.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pulso_lake_mirror.email}"
}

# Cloud SQL — catálogo Iceberg via IP privado
resource "google_project_iam_member" "lake_mirror_cloudsql" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.pulso_lake_mirror.email}"
}

# Secrets do catálogo Iceberg
resource "google_secret_manager_secret_iam_member" "lake_mirror_iceberg_secrets" {
  for_each = {
    "lake-mirror-catalog-uri" = google_secret_manager_secret.iceberg_catalog_uri.secret_id
    "lake-mirror-warehouse"   = google_secret_manager_secret.iceberg_warehouse.secret_id
  }
  secret_id = each.value
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pulso_lake_mirror.email}"
}

# Artifact Registry — pull da imagem
resource "google_artifact_registry_repository_iam_member" "lake_mirror_ar_reader" {
  project    = var.project_id
  location   = var.region
  repository = google_artifact_registry_repository.pulso.repository_id
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.pulso_lake_mirror.email}"
}

# ── IAM — serve lê do bucket de espelho ───────────────────────
resource "google_storage_bucket_iam_member" "serve_lake_mirror_viewer" {
  bucket = google_storage_bucket.lake_mirror.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.pulso_serve.email}"
}

# ── Cloud Run Job ─────────────────────────────────────────────
resource "google_cloud_run_v2_job" "lake_mirror" {
  name                = "pulso-lake-mirror"
  location            = var.region
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.pulso_lake_mirror.email
      timeout         = "1800s" # 30 min — folga para mirror + dbt em tabelas grandes
      max_retries     = 0       # fail-loud: não oculta falhas de materialização

      vpc_access {
        network_interfaces {
          network    = local.vpc_access.network
          subnetwork = local.vpc_access.subnetwork
        }
        egress = "PRIVATE_RANGES_ONLY"
      }

      volumes {
        name = "cloudsql"
        cloud_sql_instance {
          instances = [local.cloud_sql_instance]
        }
      }

      # O DuckDB materializado é gravado diretamente no bucket via FUSE.
      volumes {
        name = "lake-mirror"
        gcs {
          bucket    = google_storage_bucket.lake_mirror.name
          read_only = false
        }
      }

      containers {
        image   = local.image
        command = ["bash"]
        args = [
          "-c",
          join(" && ", [
            "python -m pulso_storage mirror",
            "dbt build --project-dir dbt --profiles-dir dbt --target dev",
          ]),
        ]

        volume_mounts {
          name       = "cloudsql"
          mount_path = "/cloudsql"
        }

        volume_mounts {
          name       = "lake-mirror"
          mount_path = "/data/lake-mirror"
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi" # DuckDB + PyIceberg durante o mirror
          }
        }

        env {
          name  = "PULSO_DBT_DUCKDB_PATH"
          value = "/data/lake-mirror/pulso_lake.duckdb"
        }

        dynamic "env" {
          for_each = local.common_env
          content {
            name  = env.key
            value = env.value
          }
        }

        dynamic "env" {
          for_each = local.iceberg_secrets
          content {
            name = env.key
            value_source {
              secret_key_ref {
                secret  = env.value
                version = "latest"
              }
            }
          }
        }
      }
    }
  }

  depends_on = [
    google_project_service.apis,
    google_sql_database_instance.iceberg_catalog,
    google_storage_bucket.lake,
    google_storage_bucket.lake_mirror,
  ]
}

# ── Scheduler — invoker ───────────────────────────────────────
resource "google_cloud_run_v2_job_iam_member" "scheduler_invokes_lake_mirror" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.lake_mirror.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pulso_scheduler.email}"
}

# ── Cloud Scheduler — a cada hora em :30 ──────────────────────
# Offset de 30 min em relação ao Soda (:00) garante que o espelho
# do hora anterior já esteja disponível quando o Soda verificar.
resource "google_cloud_scheduler_job" "lake_mirror_hourly" {
  name      = "pulso-lake-mirror-hourly"
  region    = var.region
  schedule  = "30 * * * *"
  time_zone = "Etc/UTC"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.lake_mirror.name}:run"

    oauth_token {
      service_account_email = google_service_account.pulso_scheduler.email
    }
  }

  retry_config {
    retry_count = 0
  }

  depends_on = [
    google_project_service.apis,
    google_cloud_run_v2_job_iam_member.scheduler_invokes_lake_mirror,
  ]
}
