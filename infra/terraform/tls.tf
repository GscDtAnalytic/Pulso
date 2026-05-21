# ─────────────────────────────────────────────────────────────
# PKI interna (item 5) — CA self-signed + cert do broker Redpanda.
#
# A CA assina o cert do servidor; os clientes (serviços Cloud Run)
# validam o TLS contra o CA cert. Tudo gerado no apply e guardado
# no Secret Manager. As chaves privadas vivem no state — por isso
# o state é remoto e criptografado no GCS (ver item 4).
# ─────────────────────────────────────────────────────────────

# Senha do usuário SASL/SCRAM compartilhado pelos serviços ("pulso").
resource "random_password" "kafka_sasl" {
  length  = 32
  special = false
}

# ── CA ───────────────────────────────────────────────────────
resource "tls_private_key" "ca" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "tls_self_signed_cert" "ca" {
  private_key_pem = tls_private_key.ca.private_key_pem

  subject {
    common_name  = "Pulso Internal CA"
    organization = "Pulso"
  }

  validity_period_hours = 87600 # 10 anos
  is_ca_certificate     = true

  allowed_uses = [
    "cert_signing",
    "crl_signing",
    "digital_signature",
  ]
}

# ── Cert do servidor Redpanda (Kafka API + Schema Registry) ──
resource "tls_private_key" "redpanda" {
  algorithm = "RSA"
  rsa_bits  = 2048
}

resource "tls_cert_request" "redpanda" {
  private_key_pem = tls_private_key.redpanda.private_key_pem

  subject {
    common_name  = "redpanda.pulso.internal"
    organization = "Pulso"
  }

  # O cert precisa do IP interno nos SANs: os clientes conectam pelo IP
  # e o librdkafka valida o hostname/IP contra o certificado.
  ip_addresses = [google_compute_address.redpanda_internal.address]
  dns_names    = ["redpanda.pulso.internal"]
}

resource "tls_locally_signed_cert" "redpanda" {
  cert_request_pem   = tls_cert_request.redpanda.cert_request_pem
  ca_private_key_pem = tls_private_key.ca.private_key_pem
  ca_cert_pem        = tls_self_signed_cert.ca.cert_pem

  validity_period_hours = 43800 # 5 anos

  allowed_uses = [
    "server_auth",
    "digital_signature",
    "key_encipherment",
  ]
}
