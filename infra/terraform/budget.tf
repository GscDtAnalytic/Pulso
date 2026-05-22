# ─────────────────────────────────────────────────────────────
# Budget alert de custo mensal — GCP Billing Budgets API
#
# Cria um teto de custo mensal (~$60, +50% sobre o estimado em
# COST_ANALYSIS.md de ~$40) com alertas em 50%, 90% e 100%.
# O alerta usa o mesmo canal de e-mail dos SLOs.
#
# Pré-requisito: o billing account ID deve ser passado via variável
# (não é derivável do project_id automaticamente). Configure em
# terraform.tfvars: billing_account = "XXXXXX-XXXXXX-XXXXXX".
#
# A API cloudbilling.googleapis.com deve estar habilitada no projeto.
# Adicione manualmente se necessário: gcloud services enable cloudbilling.googleapis.com
# ─────────────────────────────────────────────────────────────

resource "google_billing_budget" "pulso" {
  count = var.billing_account != "" ? 1 : 0

  billing_account = var.billing_account
  display_name    = "Pulso — teto mensal $${var.budget_usd}"

  budget_filter {
    projects = ["projects/${var.project_id}"]
  }

  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(var.budget_usd)
    }
  }

  # Alertas em 50%, 90% e 100% do orçamento.
  # threshold_percent = 1.0 → 100% (gasto real), 1.2 → 120% (previsão).
  threshold_rules {
    threshold_percent = 0.5
    spend_basis       = "CURRENT_SPEND"
  }
  threshold_rules {
    threshold_percent = 0.9
    spend_basis       = "CURRENT_SPEND"
  }
  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "CURRENT_SPEND"
  }
  threshold_rules {
    threshold_percent = 1.2
    spend_basis       = "FORECASTED_SPEND"
  }

  # Envia notificações para os alertas Cloud Monitoring (canal de e-mail).
  # Também é possível conectar a um Pub/Sub para automação (ex.: desligar serviços).
  all_updates_rule {
    monitoring_notification_channels = local.alert_channels
    disable_default_iam_recipients   = false
  }
}
