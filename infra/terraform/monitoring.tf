# ─────────────────────────────────────────────────────────────
# Observabilidade em produção (item 2)
#
# Cada serviço Cloud Run roda um sidecar GMP que raspa /metrics e
# envia ao Managed Service for Prometheus. Aqui ficam o canal de
# notificação e as alert policies — espelham governance/slo.yml e
# governance/prometheus_rules.yml, agora avaliadas pelo Cloud
# Monitoring (PromQL nativo) em vez de um Prometheus inexistente.
# ─────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────
# Uptime check da VM Redpanda
#
# O GCM faz TCP probe na porta Admin do Redpanda (9644) a cada
# minuto a partir de 3 regiões do Google. Se a porta ficar muda
# por 5 min (5 sondas consecutivas falhando em todas as regiões),
# o alerta dispara. Isso complementa os SLOs de freshness/lag:
# pega o caso onde a VM inteira está down antes do lag se acumular.
# ─────────────────────────────────────────────────────────────
resource "google_monitoring_uptime_check_config" "redpanda" {
  display_name = "Pulso — Redpanda broker TCP"
  timeout      = "10s"
  period       = "60s"

  tcp_check {
    port = 9644 # porta Admin do Redpanda (sem autenticação — apenas probe TCP)
  }

  monitored_resource {
    type = "uptime_url"
    labels = {
      project_id = var.project_id
      host       = google_compute_address.redpanda_internal.address
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_monitoring_alert_policy" "redpanda_down" {
  display_name = "PulsoRedpandaDown — broker inacessível"
  combiner     = "OR"

  conditions {
    display_name = "Redpanda TCP probe falhou"
    condition_threshold {
      filter          = "metric.type=\"monitoring.googleapis.com/uptime_check/check_passed\" AND resource.type=\"uptime_url\" AND metric.label.\"check_id\"=\"${google_monitoring_uptime_check_config.redpanda.uptime_check_id}\""
      comparison      = "COMPARISON_LT"
      threshold_value = 1
      duration        = "300s"

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_NEXT_OLDER"
        cross_series_reducer = "REDUCE_COUNT_TRUE"
        group_by_fields      = ["resource.label.host"]
      }
    }
  }

  documentation {
    content   = "O TCP probe na porta 9644 do Redpanda falhou por 5 minutos. A VM pode estar down ou reiniciando. Verifique: `gcloud compute instances describe pulso-redpanda` e `/var/log/redpanda-startup.log`."
    mime_type = "text/markdown"
  }

  notification_channels = local.alert_channels

  depends_on = [
    google_project_service.apis,
    google_monitoring_uptime_check_config.redpanda,
  ]
}

# Canal de notificação por e-mail — criado só se var.alert_email for definido.
resource "google_monitoring_notification_channel" "email" {
  count        = var.alert_email != "" ? 1 : 0
  display_name = "Pulso — alertas de SLO"
  type         = "email"
  labels = {
    email_address = var.alert_email
  }
}

locals {
  alert_channels = var.alert_email != "" ? [google_monitoring_notification_channel.email[0].id] : []
}

# ─────────────────────────────────────────────────────────────
# Alertas de SLO — métricas built-in do Cloud Run.
#
# run.googleapis.com/request_count e request_latencies são métricas
# de sistema pré-registradas no Cloud Monitoring; não requerem que
# os serviços estejam emitindo dados no momento do terraform apply.
#
# slo_freshness / slo_consumer_lag / slo_e2e_latency: alertam quando
# pulso-sink fica sem tráfego por 10 min (proxy para "sink parado").
# slo_serve_latency: alerta quando p99 HTTP do pulso-serve supera 2s.
#
# Após os serviços estarem em produção emitindo métricas GMP, os
# alertas de threshold específicos podem ser adicionados via Console
# ou segundo `terraform apply` — sem risco de erro de bootstrap.
# ─────────────────────────────────────────────────────────────

resource "google_monitoring_alert_policy" "slo_freshness" {
  display_name = "PulsoSinkDown — pulso-sink sem requisições"
  combiner     = "OR"

  conditions {
    display_name = "pulso-sink sem tráfego por 10 min"
    condition_absent {
      filter   = "metric.type=\"run.googleapis.com/request_count\" AND resource.type=\"cloud_run_revision\" AND resource.label.\"service_name\"=\"pulso-sink\""
      duration = "600s"
      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.label.service_name"]
      }
    }
  }

  documentation {
    content   = "O Cloud Run service pulso-sink ficou sem tráfego por 10 min. Se o sink estiver down, freshness do lake vai crescer e o consumer lag vai acumular. Verifique: `gcloud run services describe pulso-sink`."
    mime_type = "text/markdown"
  }

  notification_channels = local.alert_channels
  depends_on            = [google_project_service.apis]
}

resource "google_monitoring_alert_policy" "slo_consumer_lag" {
  display_name = "PulsoIngestDown — pulso-ingest sem requisições"
  combiner     = "OR"

  conditions {
    display_name = "pulso-ingest sem tráfego por 10 min"
    condition_absent {
      filter   = "metric.type=\"run.googleapis.com/request_count\" AND resource.type=\"cloud_run_revision\" AND resource.label.\"service_name\"=\"pulso-ingest\""
      duration = "600s"
      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.label.service_name"]
      }
    }
  }

  documentation {
    content   = "O Cloud Run service pulso-ingest ficou sem tráfego por 10 min. Se o producer estiver down, nenhum trade chega ao Kafka e o consumer lag deixa de existir. Verifique: `gcloud run services describe pulso-ingest`."
    mime_type = "text/markdown"
  }

  notification_channels = local.alert_channels
  depends_on            = [google_project_service.apis]
}

resource "google_monitoring_alert_policy" "slo_serve_latency" {
  display_name = "PulsoServeLatencyHigh — API p99 > 2s"
  combiner     = "OR"

  conditions {
    display_name = "HTTP latency p99 > 2s (pulso-serve)"
    condition_threshold {
      filter          = "metric.type=\"run.googleapis.com/request_latencies\" AND resource.type=\"cloud_run_revision\" AND resource.label.\"service_name\"=\"pulso-serve\""
      comparison      = "COMPARISON_GT"
      threshold_value = 2000
      duration        = "300s"
      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_PERCENTILE_99"
        group_by_fields      = ["resource.label.service_name"]
      }
    }
  }

  documentation {
    content   = "p99 da latência HTTP do pulso-serve acima de 2s por 5 min. A rota pode estar sobrecarregada (DuckDB/ksqlDB). Verifique logs: `gcloud run services logs read pulso-serve`."
    mime_type = "text/markdown"
  }

  notification_channels = local.alert_channels
  depends_on            = [google_project_service.apis]
}

resource "google_monitoring_alert_policy" "slo_e2e_latency" {
  display_name = "PulsoServeDown — pulso-serve sem requisições"
  combiner     = "OR"

  conditions {
    display_name = "pulso-serve sem tráfego por 10 min"
    condition_absent {
      filter   = "metric.type=\"run.googleapis.com/request_count\" AND resource.type=\"cloud_run_revision\" AND resource.label.\"service_name\"=\"pulso-serve\""
      duration = "600s"
      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["resource.label.service_name"]
      }
    }
  }

  documentation {
    content   = "O Cloud Run service pulso-serve ficou sem tráfego por 10 min. Verifique se a API está healthy: `curl https://<SERVE_URL>/health`. Se o serve estiver down, o dashboard e a API de candles estão indisponíveis."
    mime_type = "text/markdown"
  }

  notification_channels = local.alert_channels
  depends_on            = [google_project_service.apis]
}
