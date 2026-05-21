# ─────────────────────────────────────────────────────────────
# Helpers reutilizados em todos os serviços
# ─────────────────────────────────────────────────────────────

locals {
  # Secrets comuns a todos os serviços
  all_secrets = {
    PULSO_KAFKA_BOOTSTRAP     = google_secret_manager_secret.kafka_bootstrap.secret_id
    PULSO_SCHEMA_REGISTRY_URL = google_secret_manager_secret.schema_registry_url.secret_id
  }

  # Secrets adicionais para serviços que leem/escrevem o Iceberg
  iceberg_secrets = {
    PULSO_ICEBERG_CATALOG_URI = google_secret_manager_secret.iceberg_catalog_uri.secret_id
    PULSO_ICEBERG_WAREHOUSE   = google_secret_manager_secret.iceberg_warehouse.secret_id
  }
}

# ─────────────────────────────────────────────────────────────
# 1. pulso-ingest  — producer WebSocket → Kafka
# ─────────────────────────────────────────────────────────────
resource "google_cloud_run_v2_service" "pulso_ingest" {
  name     = "pulso-ingest"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"  # sem tráfego externo; só health check

  template {
    service_account = google_service_account.pulso_ingest.email

    annotations = {
      "autoscaling.knative.dev/minScale" = "1"
      "autoscaling.knative.dev/maxScale" = "1"
    }

    containers {
      image   = local.image
      command = ["python"]
      args    = ["-m", "pulso_ingest"]

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "0.5"
          memory = "512Mi"
        }
        cpu_idle = false  # worker contínuo; CPU sempre alocada
      }

      dynamic "env" {
        for_each = local.common_env
        content {
          name  = env.key
          value = env.value
        }
      }

      env { name = "PULSO_METRICS_PORT"; value = "8080" }

      dynamic "env" {
        for_each = local.all_secrets
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

      startup_probe {
        http_get { path = "/metrics"; port = 8080 }
        initial_delay_seconds = 15
        failure_threshold     = 5
        period_seconds        = 10
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  depends_on = [google_project_service.apis]
}

# ─────────────────────────────────────────────────────────────
# 2. pulso-sink  — Kafka → Iceberg (GCS)
# ─────────────────────────────────────────────────────────────
resource "google_cloud_run_v2_service" "pulso_sink" {
  name     = "pulso-sink"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.pulso_sink.email

    annotations = {
      "autoscaling.knative.dev/minScale"          = "1"
      "autoscaling.knative.dev/maxScale"          = "1"
      "run.googleapis.com/cloudsql-instances"     = local.cloud_sql_instance
    }

    containers {
      image   = local.image
      command = ["python"]
      args    = ["-m", "pulso_storage"]

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "1Gi"  # PyIceberg + PyArrow precisam de mais memória
        }
        cpu_idle = false
      }

      dynamic "env" {
        for_each = local.common_env
        content { name = env.key; value = env.value }
      }

      env { name = "PULSO_SINK_METRICS_PORT"; value = "8080" }

      dynamic "env" {
        for_each = merge(local.all_secrets, local.iceberg_secrets)
        content {
          name = env.key
          value_source {
            secret_key_ref { secret = env.value; version = "latest" }
          }
        }
      }

      startup_probe {
        http_get { path = "/metrics"; port = 8080 }
        initial_delay_seconds = 20
        failure_threshold     = 5
        period_seconds        = 10
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  depends_on = [
    google_project_service.apis,
    google_sql_database_instance.iceberg_catalog,
    google_storage_bucket.lake,
  ]
}

# ─────────────────────────────────────────────────────────────
# 3. pulso-anomaly-detector  — rolling window + z-score
# ─────────────────────────────────────────────────────────────
resource "google_cloud_run_v2_service" "pulso_anomaly" {
  name     = "pulso-anomaly-detector"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.pulso_anomaly.email

    annotations = {
      "autoscaling.knative.dev/minScale" = "1"
      "autoscaling.knative.dev/maxScale" = "1"
    }

    containers {
      image   = local.image
      command = ["python"]
      args    = ["services/anomaly_detector.py"]

      ports {
        container_port = 8080
      }

      resources {
        limits = { cpu = "0.5"; memory = "512Mi" }
        cpu_idle = false
      }

      dynamic "env" {
        for_each = local.common_env
        content { name = env.key; value = env.value }
      }

      env { name = "PULSO_ANOMALY_DETECTOR_METRICS_PORT"; value = "8080" }

      dynamic "env" {
        for_each = local.all_secrets
        content {
          name = env.key
          value_source {
            secret_key_ref { secret = env.value; version = "latest" }
          }
        }
      }

      startup_probe {
        http_get { path = "/metrics"; port = 8080 }
        initial_delay_seconds = 15
        failure_threshold     = 5
        period_seconds        = 10
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  depends_on = [google_project_service.apis]
}

# ─────────────────────────────────────────────────────────────
# 4. pulso-llm-explainer  — Claude API + DuckDB no GCS
# ─────────────────────────────────────────────────────────────
resource "google_cloud_run_v2_service" "pulso_llm_explainer" {
  name     = "pulso-llm-explainer"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.pulso_llm_explainer.email

    annotations = {
      "autoscaling.knative.dev/minScale" = "1"
      "autoscaling.knative.dev/maxScale" = "1"
    }

    # O DuckDB de anomalias é montado via bucket GCS (Cloud Run v2 GCS volumes).
    # Evita estado efêmero no container e sobrevive a restarts.
    volumes {
      name = "anomaly-db"
      gcs {
        bucket    = google_storage_bucket.anomaly_db.name
        read_only = false
      }
    }

    containers {
      image   = local.image
      command = ["python"]
      args    = ["services/llm_explainer.py"]

      ports {
        container_port = 8080
      }

      resources {
        limits = { cpu = "0.5"; memory = "512Mi" }
        cpu_idle = false
      }

      volume_mounts {
        name       = "anomaly-db"
        mount_path = "/data"
      }

      dynamic "env" {
        for_each = local.common_env
        content { name = env.key; value = env.value }
      }

      env { name = "PULSO_LLM_EXPLAINER_METRICS_PORT"; value = "8080" }
      env { name = "PULSO_ANOMALY_DUCKDB_PATH"; value = "/data/anomaly_explanations.duckdb" }

      dynamic "env" {
        for_each = merge(local.all_secrets, {
          PULSO_ANTHROPIC_API_KEY = google_secret_manager_secret.anthropic_api_key.secret_id
        })
        content {
          name = env.key
          value_source {
            secret_key_ref { secret = env.value; version = "latest" }
          }
        }
      }

      startup_probe {
        http_get { path = "/metrics"; port = 8080 }
        initial_delay_seconds = 15
        failure_threshold     = 5
        period_seconds        = 10
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  depends_on = [
    google_project_service.apis,
    google_storage_bucket.anomaly_db,
  ]
}

# ─────────────────────────────────────────────────────────────
# 5. pulso-serve  — FastAPI (histórico + live + WebSocket)
# ─────────────────────────────────────────────────────────────
resource "google_cloud_run_v2_service" "pulso_serve" {
  name     = "pulso-serve"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"  # ponto de entrada público

  template {
    service_account = google_service_account.pulso_serve.email

    annotations = {
      "autoscaling.knative.dev/minScale"      = "0"
      "autoscaling.knative.dev/maxScale"      = "5"
      "run.googleapis.com/cloudsql-instances" = local.cloud_sql_instance
    }

    # O DuckDB de anomalias é read-only aqui (o explainer é o único escritor).
    volumes {
      name = "anomaly-db"
      gcs {
        bucket    = google_storage_bucket.anomaly_db.name
        read_only = true
      }
    }

    containers {
      image   = local.image
      command = ["python"]
      args    = ["-m", "pulso_serve"]

      ports {
        container_port = 8080
      }

      resources {
        limits          = { cpu = "1"; memory = "512Mi" }
        cpu_idle          = true   # pode escalar a zero entre requisições
        startup_cpu_boost = true
      }

      volume_mounts {
        name       = "anomaly-db"
        mount_path = "/data"
      }

      dynamic "env" {
        for_each = local.common_env
        content { name = env.key; value = env.value }
      }

      env { name = "PORT"; value = "8080" }
      env { name = "PULSO_ANOMALY_DUCKDB_PATH"; value = "/data/anomaly_explanations.duckdb" }

      dynamic "env" {
        for_each = merge(local.all_secrets, local.iceberg_secrets)
        content {
          name = env.key
          value_source {
            secret_key_ref { secret = env.value; version = "latest" }
          }
        }
      }

      startup_probe {
        http_get { path = "/metrics"; port = 8080 }
        initial_delay_seconds = 10
        failure_threshold     = 3
        period_seconds        = 5
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  depends_on = [
    google_project_service.apis,
    google_sql_database_instance.iceberg_catalog,
    google_storage_bucket.lake,
    google_storage_bucket.anomaly_db,
  ]
}

# ─────────────────────────────────────────────────────────────
# Acesso público ao pulso-serve (Cloud Run IAM)
# ─────────────────────────────────────────────────────────────
resource "google_cloud_run_v2_service_iam_member" "serve_public" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.pulso_serve.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
