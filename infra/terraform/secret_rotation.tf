# ─────────────────────────────────────────────────────────────
# Rotação automática de secrets sensíveis (item: secret rotation)
#
# Secret Manager publica no tópico Pub/Sub quando o `next_rotation_time`
# de um secret é atingido. A partir daqui, o operador pode:
#   a) conectar um Cloud Function ao tópico para rotação automatizada, ou
#   b) tratar o evento manualmente (o tópico serve de gatilho/log).
#
# Secrets rotacionados: anthropic_api_key, kafka_sasl_password, db_password.
# Período: 90 dias. next_rotation_time: 90 dias a partir do primeiro apply.
#
# Ref: https://cloud.google.com/secret-manager/docs/rotation-recommendations
# ─────────────────────────────────────────────────────────────

data "google_project" "project" {
  project_id = var.project_id
}

# Pub/Sub topic que recebe os eventos de rotação do Secret Manager.
resource "google_pubsub_topic" "secret_rotation" {
  name    = "pulso-secret-rotation"
  project = var.project_id

  message_retention_duration = "86400s" # 24h

  depends_on = [google_project_service.apis]
}

# O Secret Manager usa uma SA gerenciada pelo Google para publicar no tópico.
# É necessário conceder roles/pubsub.publisher à SA desse projeto.
resource "google_pubsub_topic_iam_member" "secret_manager_publisher" {
  topic  = google_pubsub_topic.secret_rotation.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-secretmanager.iam.gserviceaccount.com"
}

# Subscription de pull para inspeção operacional / futura Cloud Function.
# Retenção de 7 dias; mensagens não consumidas ficam disponíveis para debug.
resource "google_pubsub_subscription" "secret_rotation_pull" {
  name    = "pulso-secret-rotation-pull"
  topic   = google_pubsub_topic.secret_rotation.name
  project = var.project_id

  message_retention_duration = "604800s" # 7 dias
  retain_acked_messages      = false
  ack_deadline_seconds       = 60

  expiration_policy {
    ttl = "" # não expira
  }
}

# Alerta Cloud Monitoring: notifica por e-mail quando uma mensagem de rotação
# chega ao tópico (proxy para "secret está vencendo, rotacione agora").
resource "google_monitoring_alert_policy" "secret_rotation_due" {
  display_name = "PulsoSecretRotationDue — secret vencendo"
  combiner     = "OR"

  conditions {
    display_name = "Mensagem de rotação no tópico Pub/Sub"
    condition_threshold {
      filter          = "metric.type=\"pubsub.googleapis.com/topic/send_message_operation_count\" AND resource.type=\"pubsub_topic\" AND resource.label.\"topic_id\"=\"${google_pubsub_topic.secret_rotation.name}\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"
      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.label.topic_id"]
      }
    }
  }

  documentation {
    content   = "Um ou mais secrets do Pulso estão com rotação vencida. Consulte `gcloud pubsub subscriptions pull pulso-secret-rotation-pull --limit=10` para ver quais secrets dispararam e rotacione as credenciais conforme o RUNBOOK_PROD.md."
    mime_type = "text/markdown"
  }

  notification_channels = local.alert_channels
  depends_on            = [google_project_service.apis, google_pubsub_topic.secret_rotation]
}
