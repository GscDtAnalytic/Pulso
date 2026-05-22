# ─────────────────────────────────────────────────────────────
# Budget alert de custo mensal — GCP Billing Budgets API
#
# Usa last_period_amount (valor do mês anterior) para evitar
# dependência de moeda — a billing account é em BRL, não USD.
# Alertas em 50%, 90%, 100% e 120% (previsão) do orçamento.
#
# Pré-requisito: billing_account configurado em terraform.tfvars.
# ─────────────────────────────────────────────────────────────

resource "google_billing_budget" "pulso" {
  count = var.billing_account != "" ? 1 : 0

  billing_account = var.billing_account
  display_name    = "Pulso monthly budget"

  budget_filter {
    projects = ["projects/${var.project_id}"]
  }

  amount {
    last_period_amount = true
  }

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
}
