# ─────────────────────────────────────────────────────────────
# Jobs de manutenção agendados (item 3)
#
# Em produção não há `make` para rodar a manutenção do lake. Sem
# isto, `expire_snapshots`/compactação nunca rodam → small-file
# explosion garantida. Cloud Run Job + Cloud Scheduler resolvem.
#
# Escopo: manutenção Iceberg (a que degrada o sistema com o tempo).
# freshness é métrica viva (ver monitoring.tf); soda/lake-mirror
# são bridges de dev e ficam fora do prod por desenho.
# ─────────────────────────────────────────────────────────────

# SA que o Cloud Scheduler usa para disparar o job.
resource "google_service_account" "pulso_scheduler" {
  account_id   = "pulso-scheduler-sa"
  display_name = "Pulso Cloud Scheduler (dispara jobs de manutenção)"
}

# ─────────────────────────────────────────────────────────────
# Job: manutenção Iceberg — `python -m pulso_storage maintain`.
# Reutiliza a SA do sink (já tem GCS objectAdmin no lake,
# cloudsql.client e acesso aos secrets de catálogo/warehouse).
# ─────────────────────────────────────────────────────────────
resource "google_cloud_run_v2_job" "iceberg_maintain" {
  name     = "pulso-iceberg-maintain"
  location = var.region

  template {
    annotations = {
      "run.googleapis.com/cloudsql-instances" = local.cloud_sql_instance
    }

    template {
      service_account = google_service_account.pulso_sink.email
      timeout         = "1800s" # 30 min — folga para compactar partições
      max_retries     = 1

      vpc_access {
        network_interfaces {
          network    = local.vpc_access.network
          subnetwork = local.vpc_access.subnetwork
        }
        egress = "PRIVATE_RANGES_ONLY"
      }

      containers {
        image   = local.image
        command = ["python"]
        args    = ["-m", "pulso_storage", "maintain"]

        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }

        dynamic "env" {
          for_each = local.common_env
          content {
            name  = env.key
            value = env.value
          }
        }

        dynamic "env" {
          for_each = merge(local.all_secrets, local.iceberg_secrets)
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

# O Scheduler precisa de run.invoker no job para dispará-lo.
resource "google_cloud_run_v2_job_iam_member" "scheduler_invokes_maintain" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.iceberg_maintain.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pulso_scheduler.email}"
}

# ─────────────────────────────────────────────────────────────
# Cloud Scheduler — dispara a manutenção diariamente às 05:00 UTC
# (depois do snapshot do disco Redpanda às 04:00).
# ─────────────────────────────────────────────────────────────
resource "google_cloud_scheduler_job" "iceberg_maintain_daily" {
  name      = "pulso-iceberg-maintain-daily"
  region    = var.region
  schedule  = "0 5 * * *"
  time_zone = "Etc/UTC"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.iceberg_maintain.name}:run"

    oauth_token {
      service_account_email = google_service_account.pulso_scheduler.email
    }
  }

  retry_config {
    retry_count = 1
  }

  depends_on = [
    google_project_service.apis,
    google_cloud_run_v2_job_iam_member.scheduler_invokes_maintain,
  ]
}
