# ─────────────────────────────────────────────────────────────
# Observabilidade em produção (item 2)
#
# Cada serviço Cloud Run roda um sidecar GMP que raspa /metrics e
# envia ao Managed Service for Prometheus. Aqui ficam o canal de
# notificação e as alert policies — espelham governance/slo.yml e
# governance/prometheus_rules.yml, agora avaliadas pelo Cloud
# Monitoring (PromQL nativo) em vez de um Prometheus inexistente.
# ─────────────────────────────────────────────────────────────

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

  # Alertas PromQL — espelham os thresholds de governance/slo.yml.
  slo_alerts = {
    freshness = {
      display = "PulsoFreshnessHigh — lake desatualizado"
      query   = "pulso_sink_freshness_seconds{table=\"bronze.trades\"} > 60"
      doc     = "bronze.trades está mais de 60s atrás do event_time. Verifique o sink e o consumer lag de trades.raw."
    }
    consumer_lag = {
      display = "PulsoConsumerLagHigh — sink atrasado"
      query   = "pulso_sink_consumer_lag > 10000"
      doc     = "O sink Iceberg está >10k mensagens atrás. Verifique throughput e capacidade do sink."
    }
    serve_latency = {
      display = "PulsoServeLatencyHigh — API p99 > 2s"
      query   = "histogram_quantile(0.99, rate(pulso_serve_http_request_seconds_bucket[5m])) > 2"
      doc     = "p99 da latência HTTP acima de 2s. A rota pode estar sobrecarregada (DuckDB/ksqlDB)."
    }
    e2e_latency = {
      display = "PulsoE2ELatencyHigh — candles além do SLO"
      query   = "pulso_sink_freshness_seconds{table=\"silver.candles\"} > 5"
      doc     = "silver.candles está >5s atrás. Verifique o ksqlDB e o pipeline de candles."
    }
  }
}

resource "google_monitoring_alert_policy" "slo" {
  for_each = local.slo_alerts

  display_name = each.value.display
  combiner     = "OR"

  conditions {
    display_name = each.value.display
    condition_prometheus_query_language {
      query               = each.value.query
      duration            = "300s" # for: 5m
      evaluation_interval = "60s"
    }
  }

  documentation {
    content   = each.value.doc
    mime_type = "text/markdown"
  }

  notification_channels = local.alert_channels

  depends_on = [google_project_service.apis]
}
