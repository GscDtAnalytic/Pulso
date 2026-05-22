# Instância Postgres para o catálogo JDBC do Iceberg.
# PyIceberg (SqlCatalog) e Trino (iceberg.catalog.type=jdbc) compartilham o mesmo banco.
#
# Hardening (item 8): sem IP público — só IP privado via Private Service Access;
# backups diários + point-in-time recovery; deletion_protection ligada. Zona
# única (sem HA regional) é uma escolha portfolio-grade; failover = restore.

# ─────────────────────────────────────────────────────────────
# Private Service Access — peering da VPC com os serviços Google,
# para que o Cloud SQL tenha IP privado (sem exposição pública).
# ─────────────────────────────────────────────────────────────
resource "google_compute_global_address" "cloudsql_private_range" {
  name          = "pulso-cloudsql-private-range"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = "default"

  depends_on = [google_project_service.apis]
}

resource "google_service_networking_connection" "cloudsql_private" {
  network                 = "projects/${var.project_id}/global/networks/default"
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.cloudsql_private_range.name]
}

resource "google_sql_database_instance" "iceberg_catalog" {
  name             = "pulso-catalog-${var.environment}"
  database_version = "POSTGRES_15"
  region           = var.region

  settings {
    tier = "db-f1-micro" # ~$10/mês; adequado para catálogo de metadados Iceberg

    backup_configuration {
      enabled    = true
      start_time = "03:00"
      # Point-in-time recovery: permite restaurar a qualquer instante via WAL.
      point_in_time_recovery_enabled = true
      transaction_log_retention_days = 7
    }

    ip_configuration {
      # Sem IP público — acesso só pela VPC (Cloud SQL Auth Proxy sobre IP privado).
      ipv4_enabled    = false
      private_network = "projects/${var.project_id}/global/networks/default"
    }

    database_flags {
      name  = "max_connections"
      value = "100"
    }
  }

  # Produção: protege contra `terraform destroy` acidental do catálogo.
  deletion_protection = true

  depends_on = [
    google_project_service.apis,
    google_service_networking_connection.cloudsql_private,
  ]
}

resource "google_sql_database" "pulso" {
  name     = "pulso"
  instance = google_sql_database_instance.iceberg_catalog.name
}

# Banco do Marquez (item 11) — lineage; compartilha a instância para poupar custo.
resource "google_sql_database" "marquez" {
  name     = "marquez"
  instance = google_sql_database_instance.iceberg_catalog.name
}

resource "google_sql_user" "pulso" {
  name     = "pulso"
  instance = google_sql_database_instance.iceberg_catalog.name
  password = random_password.db_password.result
}

# Marquez usa "marquez" como username fixo; mesma senha gerenciada pelo secret.
resource "google_sql_user" "marquez" {
  name     = "marquez"
  instance = google_sql_database_instance.iceberg_catalog.name
  password = random_password.db_password.result
}
