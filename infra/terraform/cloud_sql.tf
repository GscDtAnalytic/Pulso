# Instância Postgres para o catálogo JDBC do Iceberg.
# PyIceberg (SqlCatalog) e Trino (iceberg.catalog.type=jdbc) compartilham o mesmo banco.

resource "google_sql_database_instance" "iceberg_catalog" {
  name             = "pulso-catalog-${var.environment}"
  database_version = "POSTGRES_15"
  region           = var.region

  settings {
    tier = "db-f1-micro"  # ~$10/mês; adequado para catálogo de metadados Iceberg

    backup_configuration {
      enabled    = true
      start_time = "03:00"
    }

    ip_configuration {
      ipv4_enabled = true  # acesso via Cloud SQL Auth Proxy (unix socket no Cloud Run)
    }

    database_flags {
      name  = "max_connections"
      value = "100"
    }
  }

  deletion_protection = false  # portfolio — habilitar em produção real

  depends_on = [google_project_service.apis]
}

resource "google_sql_database" "pulso" {
  name     = "pulso"
  instance = google_sql_database_instance.iceberg_catalog.name
}

resource "google_sql_user" "pulso" {
  name     = "pulso"
  instance = google_sql_database_instance.iceberg_catalog.name
  password = random_password.db_password.result
}
