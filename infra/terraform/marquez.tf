# ─────────────────────────────────────────────────────────────
# Marquez — backend de data lineage (item 11)
#
# Recebe os eventos OpenLineage emitidos pelo sink e pelo producer.
# Stateless: o estado vive no Postgres (banco `marquez` na instância
# Cloud SQL compartilhada). Conecta pelo IP privado do Cloud SQL —
# sem proxy/unix-socket, já que a instância tem IP privado (item 8).
# ─────────────────────────────────────────────────────────────

resource "google_service_account" "pulso_marquez" {
  account_id   = "pulso-marquez-sa"
  display_name = "Pulso Marquez (data lineage)"
}

# Marquez lê a senha do Postgres do Secret Manager.
resource "google_secret_manager_secret_iam_member" "marquez_db_password" {
  secret_id = google_secret_manager_secret.db_password.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pulso_marquez.email}"
}

resource "google_cloud_run_v2_service" "marquez" {
  name     = "pulso-marquez"
  location = var.region
  # Só serviços internos (sink, ingest) emitem lineage — sem tráfego externo.
  ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.pulso_marquez.email

    annotations = {
      "autoscaling.knative.dev/minScale" = "1"
      "autoscaling.knative.dev/maxScale" = "2"
    }

    # Egress para a VPC — alcança o IP privado do Cloud SQL.
    vpc_access {
      network_interfaces {
        network    = local.vpc_access.network
        subnetwork = local.vpc_access.subnetwork
      }
      egress = "PRIVATE_RANGES_ONLY"
    }

    containers {
      image = "marquezproject/marquez:0.50.0"

      ports {
        container_port = 5000 # porta da API do Marquez
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "1Gi" # app Java + Flyway
        }
      }

      env {
        name  = "POSTGRES_HOST"
        value = google_sql_database_instance.iceberg_catalog.private_ip_address
      }
      env {
        name  = "POSTGRES_PORT"
        value = "5432"
      }
      env {
        name  = "POSTGRES_DB"
        value = google_sql_database.marquez.name
      }
      env {
        name  = "POSTGRES_USER"
        value = google_sql_user.pulso.name
      }
      env {
        name = "POSTGRES_PASSWORD"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.db_password.secret_id
            version = "latest"
          }
        }
      }
      env {
        name  = "MARQUEZ_PORT"
        value = "5000"
      }
      env {
        name  = "MARQUEZ_ADMIN_PORT"
        value = "5001"
      }

      # Marquez (Java + migrações Flyway) leva ~1 min para subir.
      startup_probe {
        tcp_socket {
          port = 5000
        }
        initial_delay_seconds = 30
        failure_threshold     = 10
        period_seconds        = 10
        timeout_seconds       = 5
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  depends_on = [
    google_project_service.apis,
    google_sql_database.marquez,
    google_service_networking_connection.cloudsql_private,
  ]
}

# Ingress interno + allUsers: alcançável só por serviços dentro da VPC
# (o sink e o producer que emitem OpenLineage).
resource "google_cloud_run_v2_service_iam_member" "marquez_internal" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.marquez.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
