# Os recursos google_secret_manager_secret criam o container do secret.
# Os valores (versions) são populados fora do Terraform:
#   gcloud secrets versions add pulso-kafka-bootstrap --data-file=-  <<< "seed-xxx:9092"
#
# Exceção: iceberg_catalog_uri e db_password são geridos aqui mesmo.

resource "google_secret_manager_secret" "kafka_bootstrap" {
  secret_id = "pulso-kafka-bootstrap"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret" "schema_registry_url" {
  secret_id = "pulso-schema-registry-url"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret" "ksqldb_url" {
  secret_id = "pulso-ksqldb-url"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret" "anthropic_api_key" {
  secret_id = "pulso-anthropic-api-key"
  replication {
    auto {}
  }
  topics {
    name = google_pubsub_topic.secret_rotation.id
  }
  rotation {
    rotation_period    = "7776000s" # 90 dias
    next_rotation_time = "2026-08-20T00:00:00Z"
  }
  depends_on = [google_project_service.apis, google_pubsub_topic.secret_rotation]
}

# Gerado pelo Terraform; URI completo com host Cloud SQL unix socket.
resource "google_secret_manager_secret" "iceberg_catalog_uri" {
  secret_id = "pulso-iceberg-catalog-uri"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "iceberg_catalog_uri" {
  secret = google_secret_manager_secret.iceberg_catalog_uri.id
  # Conexão direta ao IP privado do Cloud SQL via VPC (egress PRIVATE_RANGES_ONLY).
  # O Cloud SQL não tem IP público, então o Auth Proxy embutido do Cloud Run (que usa
  # o caminho público) não cria o unix socket. Mesmo método TCP que o Marquez usa.
  secret_data = "postgresql+psycopg2://pulso:${random_password.db_password.result}@${google_sql_database_instance.iceberg_catalog.private_ip_address}:5432/pulso"
}

resource "google_secret_manager_secret" "iceberg_warehouse" {
  secret_id = "pulso-iceberg-warehouse"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "iceberg_warehouse" {
  secret      = google_secret_manager_secret.iceberg_warehouse.id
  secret_data = "gs://${google_storage_bucket.lake.name}/warehouse"
}

# Kafka e Schema Registry são derivados automaticamente do IP interno da VM Redpanda.
resource "google_secret_manager_secret_version" "kafka_bootstrap" {
  secret      = google_secret_manager_secret.kafka_bootstrap.id
  secret_data = "${google_compute_address.redpanda_internal.address}:9092"
  depends_on  = [google_compute_address.redpanda_internal]
}

resource "google_secret_manager_secret_version" "schema_registry_url" {
  secret = google_secret_manager_secret.schema_registry_url.id
  # HTTPS: o listener externo do Schema Registry tem TLS (item 5).
  secret_data = "https://${google_compute_address.redpanda_internal.address}:8081"
  depends_on  = [google_compute_address.redpanda_internal]
}

resource "google_secret_manager_secret_version" "ksqldb_url" {
  secret      = google_secret_manager_secret.ksqldb_url.id
  secret_data = "http://${google_compute_address.redpanda_internal.address}:8088"
  depends_on  = [google_compute_address.redpanda_internal]
}

resource "google_secret_manager_secret_version" "anthropic_api_key" {
  count       = var.anthropic_api_key != "" ? 1 : 0
  secret      = google_secret_manager_secret.anthropic_api_key.id
  secret_data = var.anthropic_api_key
}

# ─────────────────────────────────────────────────────────────
# Segurança do barramento (item 5) — TLS + SASL/SCRAM.
# CA cert, cert/chave do servidor e senha SASL geridos no Terraform.
# ─────────────────────────────────────────────────────────────

# CA cert (público) — clientes validam o TLS contra ele.
resource "google_secret_manager_secret" "tls_ca" {
  secret_id = "pulso-tls-ca"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "tls_ca" {
  secret      = google_secret_manager_secret.tls_ca.id
  secret_data = tls_self_signed_cert.ca.cert_pem
}

# Cert do servidor Redpanda — só a VM do broker lê.
resource "google_secret_manager_secret" "redpanda_cert" {
  secret_id = "pulso-redpanda-cert"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "redpanda_cert" {
  secret      = google_secret_manager_secret.redpanda_cert.id
  secret_data = tls_locally_signed_cert.redpanda.cert_pem
}

# Chave privada do servidor Redpanda — só a VM do broker lê.
resource "google_secret_manager_secret" "redpanda_key" {
  secret_id = "pulso-redpanda-key"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "redpanda_key" {
  secret      = google_secret_manager_secret.redpanda_key.id
  secret_data = tls_private_key.redpanda.private_key_pem
}

# Senha do usuário SASL/SCRAM "pulso".
resource "google_secret_manager_secret" "kafka_sasl_password" {
  secret_id = "pulso-kafka-sasl-password"
  replication {
    auto {}
  }
  topics {
    name = google_pubsub_topic.secret_rotation.id
  }
  rotation {
    rotation_period    = "7776000s" # 90 dias
    next_rotation_time = "2026-08-20T00:00:00Z"
  }
  depends_on = [google_project_service.apis, google_pubsub_topic.secret_rotation]
}

resource "google_secret_manager_secret_version" "kafka_sasl_password" {
  secret      = google_secret_manager_secret.kafka_sasl_password.id
  secret_data = random_password.kafka_sasl.result
}

# Senha do Postgres em texto puro — usada pelo Marquez (item 11), que conecta
# pelo IP privado do Cloud SQL (host:port), não pelo proxy/unix-socket.
resource "google_secret_manager_secret" "db_password" {
  secret_id = "pulso-db-password"
  replication {
    auto {}
  }
  topics {
    name = google_pubsub_topic.secret_rotation.id
  }
  rotation {
    rotation_period    = "7776000s" # 90 dias
    next_rotation_time = "2026-08-20T00:00:00Z"
  }
  depends_on = [google_project_service.apis, google_pubsub_topic.secret_rotation]
}

resource "google_secret_manager_secret_version" "db_password" {
  secret      = google_secret_manager_secret.db_password.id
  secret_data = random_password.db_password.result
}
