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

# Marquez lê a senha do Postgres e o arquivo de config do Secret Manager.
resource "google_secret_manager_secret_iam_member" "marquez_db_password" {
  secret_id = google_secret_manager_secret.db_password.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pulso_marquez.email}"
}

# Config do Marquez como secret — permite que POSTGRES_* env vars sejam
# substituídas em runtime pelo Dropwizard (${VAR:-default}).
# O marquez.dev.yml embarcado na imagem tem user/password hardcoded como
# "marquez"; aqui criamos um config próprio que lê do Secret Manager.
resource "google_secret_manager_secret" "marquez_config" {
  secret_id = "pulso-marquez-config"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "marquez_config" {
  secret = google_secret_manager_secret.marquez_config.id
  secret_data = <<-YAML
    server:
      applicationConnectors:
      - type: http
        port: $${MARQUEZ_PORT:-5000}
        httpCompliance: RFC7230_LEGACY
      adminConnectors:
      - type: http
        port: $${MARQUEZ_ADMIN_PORT:-5001}
    db:
      driverClass: org.postgresql.Driver
      url: jdbc:postgresql://$${POSTGRES_HOST}:$${POSTGRES_PORT:-5432}/$${POSTGRES_DB:-marquez}
      user: $${POSTGRES_USER:-marquez}
      password: $${POSTGRES_PASSWORD}
      properties:
        charSet: UTF-8
      minSize: 2
      maxSize: 8
      initialSize: 2
    migrateOnStartup: true
    graphql:
      enabled: true
    logging:
      level: INFO
      appenders:
        - type: console
    search:
      enabled: false
  YAML
}

resource "google_secret_manager_secret_iam_member" "marquez_config_reader" {
  secret_id = google_secret_manager_secret.marquez_config.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pulso_marquez.email}"
}

resource "google_cloud_run_v2_service" "marquez" {
  name                = "pulso-marquez"
  location            = var.region
  deletion_protection = false
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

    # Monta o marquez.yml com substituição de env vars (${VAR} → valor real).
    # O marquez.dev.yml embarcado na imagem hardcoda user/password; este
    # secret permite injetar as credenciais reais em runtime via Dropwizard.
    volumes {
      name = "marquez-config"
      secret {
        secret = google_secret_manager_secret.marquez_config.secret_id
        items {
          version = "latest"
          path    = "marquez.yml"
        }
      }
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

      volume_mounts {
        name       = "marquez-config"
        mount_path = "/etc/marquez"
      }

      env {
        name  = "MARQUEZ_CONFIG"
        value = "/etc/marquez/marquez.yml"
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
        value = google_sql_user.marquez.name
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

      startup_probe {
        tcp_socket {
          port = 5000
        }
        initial_delay_seconds = 90
        failure_threshold     = 30
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
    google_sql_user.marquez,
    google_service_networking_connection.cloudsql_private,
    google_secret_manager_secret_version.marquez_config,
    google_secret_manager_secret_iam_member.marquez_config_reader,
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
