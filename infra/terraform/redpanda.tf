# ─────────────────────────────────────────────────────────────
# Redpanda (Kafka-compatible broker) + ksqlDB — VM única no GCE.
#
# Decisão (item 1 de prontidão): portfolio-grade. Não é HA real
# (nó único, 1 zona), mas mitigado por:
#   • disco de DADOS dedicado — sobrevive à recriação da VM;
#   • snapshot diário do disco de dados — RPO de 24h;
#   • automatic_restart do GCE — self-healing em crash/host failure.
# RTO/RPO e procedimento de restore: ver RUNBOOK_PROD.md §1.
# Para HA real, migrar para 3 nós ou Redpanda Cloud.
# ─────────────────────────────────────────────────────────────

# API Compute habilitada em main.tf

# Service Account da VM (acesso mínimo: apenas logs)
resource "google_service_account" "redpanda" {
  account_id   = "pulso-redpanda-sa"
  display_name = "Pulso Redpanda broker VM"
}

resource "google_project_iam_member" "redpanda_logs" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.redpanda.email}"
}

# IP interno estático — garantia de que o endereço não muda após restart
resource "google_compute_address" "redpanda_internal" {
  name         = "redpanda-internal-ip"
  subnetwork   = "default"
  address_type = "INTERNAL"
  region       = var.region

  depends_on = [google_project_service.apis]
}

# ─────────────────────────────────────────────────────────────
# Disco de dados dedicado — WAL/segments do Redpanda + state do
# ksqlDB. Separado do boot disk: `auto_delete` implícito é false
# para discos standalone, então sobrevive à destruição da VM.
# É este disco (não o boot) que recebe o snapshot diário.
# ─────────────────────────────────────────────────────────────
resource "google_compute_disk" "redpanda_data" {
  name = "pulso-redpanda-data"
  type = "pd-ssd"
  zone = "${var.region}-a"
  size = 50 # GB — WAL + segments + state do ksqlDB

  depends_on = [google_project_service.apis]
}

# Política de snapshot diário — RPO de 24h para a fonte da verdade.
# Snapshots são incrementais; retenção de 7 dias mantém o custo baixo.
resource "google_compute_resource_policy" "redpanda_snapshot" {
  name   = "pulso-redpanda-snapshot-daily"
  region = var.region

  snapshot_schedule_policy {
    schedule {
      daily_schedule {
        days_in_cycle = 1
        start_time    = "04:00" # UTC — fora do horário de pico
      }
    }
    retention_policy {
      max_retention_days    = 7
      on_source_disk_delete = "KEEP_AUTO_SNAPSHOTS"
    }
    snapshot_properties {
      labels            = { app = "pulso", role = "redpanda" }
      storage_locations = [var.region]
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_compute_disk_resource_policy_attachment" "redpanda_data_snapshot" {
  name = google_compute_resource_policy.redpanda_snapshot.name
  disk = google_compute_disk.redpanda_data.name
  zone = "${var.region}-a"
}

# VM com Redpanda + ksqlDB (Docker)
resource "google_compute_instance" "redpanda" {
  name         = "pulso-redpanda"
  machine_type = "e2-standard-2" # 2 vCPU, 8 GB RAM — cobre Redpanda + ksqlDB
  zone         = "${var.region}-a"

  tags = ["redpanda"]

  # Self-healing: o GCE reinicia a VM automaticamente em crash do guest
  # ou falha de host, e faz live-migration em manutenção planejada.
  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    preemptible         = false
    provisioning_model  = "STANDARD"
  }

  boot_disk {
    auto_delete = true # o boot disk é descartável: o startup script reinstala tudo
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 20 # GB — só OS + imagens Docker; os dados ficam no disco dedicado
      type  = "pd-balanced"
    }
  }

  # Disco de dados — persiste entre recriações da VM.
  attached_disk {
    source      = google_compute_disk.redpanda_data.id
    device_name = "redpanda-data"
    mode        = "READ_WRITE"
  }

  network_interface {
    network    = "default"
    network_ip = google_compute_address.redpanda_internal.address

    # IP externo efêmero apenas para acesso SSH via IAP / bootstrap inicial.
    # Cloud Run acessa apenas pelo IP interno (VPC egress).
    access_config {}
  }

  service_account {
    email  = google_service_account.redpanda.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    enable-oslogin = "TRUE"
  }

  # allow_stopping_for_update: trocar o startup script não força recriar a VM
  allow_stopping_for_update = true

  metadata_startup_script = <<-SCRIPT
    #!/bin/bash
    set -euo pipefail
    exec > /var/log/redpanda-startup.log 2>&1

    INTERNAL_IP="${google_compute_address.redpanda_internal.address}"
    DATA_DEV="/dev/disk/by-id/google-redpanda-data"
    DATA_MNT="/var/lib/redpanda"

    # ── Disco de dados ──────────────────────────────────────────
    # Formata só na primeira vez (VM nova com disco em branco). Numa
    # recriação da VM o disco já tem dados — apenas remonta.
    if ! blkid "$DATA_DEV"; then
      mkfs.ext4 -F -L redpanda-data "$DATA_DEV"
    fi
    mkdir -p "$DATA_MNT"
    if ! grep -q "$DATA_MNT" /etc/fstab; then
      echo "LABEL=redpanda-data $DATA_MNT ext4 defaults,nofail 0 2" >> /etc/fstab
    fi
    mount -a
    mkdir -p "$DATA_MNT/data" "$DATA_MNT/ksqldb"
    # ksqlDB roda como uid 1000 no container Confluent
    chown 1000:1000 "$DATA_MNT/ksqldb"

    # ── Certificados TLS + senha SASL (Secret Manager) ─────────
    # Lê secrets via token do metadata server (sem instalar o gcloud).
    get_secret() {
      local name="$1" token
      token=$(curl -s -H "Metadata-Flavor: Google" \
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" \
        | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
      curl -s -H "Authorization: Bearer $token" \
        "https://secretmanager.googleapis.com/v1/projects/${var.project_id}/secrets/$name/versions/latest:access" \
        | python3 -c "import sys,json,base64; print(base64.b64decode(json.load(sys.stdin)['payload']['data']).decode())"
    }

    install -d -m 0755 /etc/redpanda/certs
    get_secret pulso-redpanda-cert > /etc/redpanda/certs/server.crt
    get_secret pulso-redpanda-key  > /etc/redpanda/certs/server.key
    get_secret pulso-tls-ca        > /etc/redpanda/certs/ca.crt
    SASL_PW=$(get_secret pulso-kafka-sasl-password)

    # ── Redpanda ────────────────────────────────────────────────
    curl -1sLf 'https://dl.redpanda.com/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | sudo -E bash
    sudo apt-get install -y redpanda

    sudo chown -R redpanda:redpanda /etc/redpanda/certs "$DATA_MNT/data"
    sudo chmod 600 /etc/redpanda/certs/server.key

    # Cluster config bootstrap: SASL ligado, 'pulso' como superuser.
    # Aplicado só na primeira formação do cluster; depois persiste no disco.
    # kafka_enable_authorization: false — autenticação SASL continua exigida no
    # listener externo, mas a *autorização* (ACLs) fica desligada. Necessário
    # porque o listener interno loopback é 'authentication_method: none' (principal
    # anônimo): com ACLs ligadas, clientes locais confiáveis (ksqlDB, criação de
    # tópicos pelo rpk) seriam barrados. Há um único usuário (pulso, superuser),
    # então ACLs não davam granularidade — só bloqueavam o loopback.
    printf 'enable_sasl: true\nkafka_enable_authorization: false\nsuperusers:\n  - pulso\n' \
      | sudo tee /etc/redpanda/.bootstrap.yaml > /dev/null

    sudo rpk redpanda config set redpanda.data_directory "$DATA_MNT/data"
    sudo rpk redpanda config bootstrap --self "$INTERNAL_IP" --ips "$INTERNAL_IP"

    # Dois listeners Kafka: 'internal' loopback em texto claro (ksqlDB, no
    # mesmo host) e 'external' com TLS+SASL no IP interno (clientes Cloud Run).
    sudo rpk redpanda config set redpanda.kafka_api \
      '[{name: internal, address: 127.0.0.1, port: 29092, authentication_method: none},{name: external, address: 0.0.0.0, port: 9092, authentication_method: sasl}]'
    sudo rpk redpanda config set redpanda.advertised_kafka_api \
      "[{name: internal, address: 127.0.0.1, port: 29092},{name: external, address: $INTERNAL_IP, port: 9092}]"
    sudo rpk redpanda config set redpanda.kafka_api_tls \
      '[{name: external, enabled: true, cert_file: /etc/redpanda/certs/server.crt, key_file: /etc/redpanda/certs/server.key, truststore_file: /etc/redpanda/certs/ca.crt, require_client_auth: false}]'

    # Schema Registry: listener interno (ksqlDB, loopback) + externo com TLS.
    sudo rpk redpanda config set schema_registry.schema_registry_api \
      '[{name: internal, address: 127.0.0.1, port: 18081},{name: external, address: 0.0.0.0, port: 8081}]'
    sudo rpk redpanda config set schema_registry.schema_registry_api_tls \
      '[{name: external, enabled: true, cert_file: /etc/redpanda/certs/server.crt, key_file: /etc/redpanda/certs/server.key, truststore_file: /etc/redpanda/certs/ca.crt}]'

    # Cliente Kafka interno do Schema Registry e do HTTP Proxy → listener loopback
    # 'none'. Sem isto o SR conecta no listener default (SASL) sem credenciais e
    # falha com 'illegal_sasl_state' ao ler/gravar o tópico interno '_schemas',
    # quebrando registro de schema para ksqlDB e para o producer.
    sudo rpk redpanda config set schema_registry_client.brokers \
      '[{address: 127.0.0.1, port: 29092}]'
    sudo rpk redpanda config set pandaproxy_client.brokers \
      '[{address: 127.0.0.1, port: 29092}]'

    sudo systemctl enable redpanda
    sudo systemctl start redpanda

    # Aguarda o broker pelo listener interno (loopback, sem SASL).
    for i in $(seq 1 30); do
      rpk cluster info --brokers 127.0.0.1:29092 >/dev/null 2>&1 && break || sleep 5
    done

    # Cria o usuário SCRAM 'pulso' via Admin API (porta 9644, sem SASL).
    rpk acl user create pulso -p "$SASL_PW" --mechanism SCRAM-SHA-256 || true

    # Tópicos criados pelo listener interno em texto claro.
    # Origem/eventos: retenção de 7 dias — janela de replay Kappa ampla; o sink
    # pode ficar fora por dias sem perder a fonte da verdade (trades.raw).
    for TOPIC in trades.raw trades.raw.dlq orderbook.delta \
                 events.anomaly events.anomaly.dlq; do
      rpk topic create "$TOPIC" --brokers 127.0.0.1:29092 \
        --topic-config retention.ms=604800000 2>/dev/null || true
    done
    # Candles: derivados (reconstrutíveis de trades.raw e do silver Iceberg) —
    # 24h cobre o feed live do dashboard.
    for TOPIC in candles.m1 candles.m5 candles.h1; do
      rpk topic create "$TOPIC" --brokers 127.0.0.1:29092 \
        --topic-config retention.ms=86400000 2>/dev/null || true
    done

    # ── Docker (para ksqlDB) ─────────────────────────────────────
    curl -fsSL https://get.docker.com | bash

    # ── ksqlDB ──────────────────────────────────────────────────
    # --network host: ksqlDB fala com Redpanda/SR pelos listeners loopback
    # em texto claro (mesmo host, sem tráfego na rede → sem necessidade de TLS).
    docker run -d \
      --name ksqldb-server \
      --restart unless-stopped \
      --network host \
      -v "$DATA_MNT/ksqldb:/var/lib/ksqldb" \
      -e KSQL_BOOTSTRAP_SERVERS="127.0.0.1:29092" \
      -e KSQL_KSQL_SCHEMA_REGISTRY_URL="http://127.0.0.1:18081" \
      -e KSQL_KSQL_LISTENERS="http://0.0.0.0:8088" \
      -e KSQL_KSQL_SERVICE_ID="pulso_" \
      -e KSQL_KSQL_STREAMS_STATE_DIR="/var/lib/ksqldb" \
      -e KSQL_KSQL_STREAMS_AUTO_OFFSET_RESET="earliest" \
      confluentinc/ksqldb-server:0.29.0

    echo "Redpanda (TLS+SASL) + ksqlDB iniciados em $INTERNAL_IP"
  SCRIPT

  depends_on = [
    google_project_service.apis,
    google_compute_address.redpanda_internal,
    google_compute_disk.redpanda_data,
  ]
}

# ── Firewall ─────────────────────────────────────────────────────────────────

# Kafka (9092), Schema Registry (8081) e ksqlDB (8088) — apenas tráfego interno VPC
resource "google_compute_firewall" "redpanda_internal" {
  name    = "allow-redpanda-internal"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["9092", "8081", "8088"]
  }

  # RFC-1918 cobre os IPs internos do Cloud Run com VPC egress
  source_ranges = ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]
  target_tags   = ["redpanda"]

  depends_on = [google_project_service.apis]
}

# SSH via IAP (Identity-Aware Proxy) — sem expor porta 22 na internet
resource "google_compute_firewall" "redpanda_iap_ssh" {
  name    = "allow-redpanda-iap-ssh"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["35.235.240.0/20"] # IP range do IAP do Google
  target_tags   = ["redpanda"]

  depends_on = [google_project_service.apis]
}
