# ─────────────────────────────────────────────────────────────
# Qualidade de dado em produção — Cloud Run Job + Cloud Scheduler
#
# O job `pulso-soda-check` roda a cada hora:
#   1. `python -m pulso_storage mirror`  — espelha Iceberg GCS → DuckDB /tmp
#   2. `dbt build --target dev`          — recria os marts analíticos no DuckDB
#   3. `soda scan -d pulso_prod ...`     — valida OHLC, gaps, volume, unicidade
#
# Exit ≠ 0 em qualquer etapa → Cloud Run marca a execução como FAILED →
# log-based metric → alerta PulsoSodaCheckFailed.
#
# Princípio #4: fail-loud (max_retries=0: não mascara falhas com retry).
# ─────────────────────────────────────────────────────────────

# ── Service Account ───────────────────────────────────────────
resource "google_service_account" "pulso_soda" {
  account_id   = "pulso-soda-sa"
  display_name = "Pulso Soda Quality Check job"
}

# Leitura do lake (mirror Iceberg → DuckDB)
resource "google_storage_bucket_iam_member" "soda_lake_viewer" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.pulso_soda.email}"
}

# Acesso ao Cloud SQL (catálogo Iceberg via proxy unix socket)
resource "google_project_iam_member" "soda_cloudsql" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.pulso_soda.email}"
}

# Secrets do catálogo Iceberg — catalog_uri e warehouse
resource "google_secret_manager_secret_iam_member" "soda_secrets" {
  for_each = {
    "soda-catalog-uri" = google_secret_manager_secret.iceberg_catalog_uri.secret_id
    "soda-warehouse"   = google_secret_manager_secret.iceberg_warehouse.secret_id
  }
  secret_id = each.value
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pulso_soda.email}"
}

# ── Cloud Run Job ─────────────────────────────────────────────
resource "google_cloud_run_v2_job" "soda_check" {
  name     = "pulso-soda-check"
  location = var.region

  template {
    template {
      service_account = google_service_account.pulso_soda.email
      timeout         = "1800s" # 30 min — folga para espelhar tabelas grandes
      max_retries     = 0       # fail-loud: não oculta falhas de qualidade com retry

      vpc_access {
        network_interfaces {
          network    = local.vpc_access.network
          subnetwork = local.vpc_access.subnetwork
        }
        egress = "PRIVATE_RANGES_ONLY"
      }

      # Cloud Run v2 Jobs conectam ao Cloud SQL via volume (anotação não é suportada em v2).
      volumes {
        name = "cloudsql"
        cloud_sql_instance {
          instances = [local.cloud_sql_instance]
        }
      }

      containers {
        volume_mounts {
          name       = "cloudsql"
          mount_path = "/cloudsql"
        }
        image   = local.image
        command = ["bash"]
        args = [
          "-c",
          join(" && ", [
            "python -m pulso_storage mirror",
            "dbt build --project-dir dbt --profiles-dir dbt --target dev",
            "soda scan -d pulso_prod -c governance/soda/datasource_prod.yml governance/soda/",
          ]),
        ]

        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi" # DuckDB pode usar bastante memória no mirror de partições grandes
          }
        }

        # DuckDB path fixo no container (referenciado pelo datasource_prod.yml)
        env {
          name  = "PULSO_DBT_DUCKDB_PATH"
          value = "/tmp/pulso_lake.duckdb"
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
  ]
}

# O Scheduler existente pode disparar este job também —
# usamos a mesma SA pulso_scheduler (já existe em maintenance.tf).
resource "google_cloud_run_v2_job_iam_member" "scheduler_invokes_soda" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.soda_check.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pulso_scheduler.email}"
}

# ── Cloud Scheduler — a cada hora ────────────────────────────
resource "google_cloud_scheduler_job" "soda_check_hourly" {
  name      = "pulso-soda-check-hourly"
  region    = var.region
  schedule  = "0 * * * *"
  time_zone = "Etc/UTC"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.soda_check.name}:run"

    oauth_token {
      service_account_email = google_service_account.pulso_scheduler.email
    }
  }

  retry_config {
    retry_count = 0 # fail-loud: não dispara novamente se o job anterior falhou
  }

  depends_on = [
    google_project_service.apis,
    google_cloud_run_v2_job_iam_member.scheduler_invokes_soda,
  ]
}

# ── Alerta de falha de qualidade ──────────────────────────────

# Métrica baseada em log: conta entradas ERROR+ do job soda-check.
# Quando o soda scan ou o dbt/mirror falham, o Cloud Run marca a
# execução como FAILED e emite uma entrada com severity=ERROR.
resource "google_logging_metric" "soda_failures" {
  name   = "pulso-soda-check-failures"
  filter = <<-EOT
    resource.type="cloud_run_job"
    resource.labels.job_name="pulso-soda-check"
    severity>=ERROR
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    display_name = "Pulso Soda Check Failures"
  }

  depends_on = [google_project_service.apis]
}

resource "google_monitoring_alert_policy" "soda_check_failed" {
  display_name = "PulsoSodaCheckFailed — qualidade de dado em prod degradada"
  combiner     = "OR"

  conditions {
    display_name = "Soda scan registrou erro em prod"
    condition_threshold {
      filter          = "metric.type=\"logging.googleapis.com/user/pulso-soda-check-failures\" AND resource.type=\"cloud_run_job\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"

      aggregations {
        alignment_period   = "3600s" # janela de 1h — igual ao intervalo do job
        per_series_aligner = "ALIGN_COUNT"
      }
    }
  }

  documentation {
    content   = "O Soda scan de qualidade de dado em produção falhou. Verifique os logs do Cloud Run Job `pulso-soda-check` (Cloud Console → Cloud Run → Jobs → pulso-soda-check) para ver quais checks quebraram. Causas comuns: OHLC inconsistente, gap de minuto em candles M1, trade_id duplicado."
    mime_type = "text/markdown"
  }

  notification_channels = local.alert_channels

  depends_on = [
    google_project_service.apis,
    google_logging_metric.soda_failures,
  ]
}
