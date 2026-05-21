# ─────────────────────────────────────────────────────────────
# Bucket principal do lakehouse Iceberg (bronze + silver + gold)
# ─────────────────────────────────────────────────────────────
resource "google_storage_bucket" "lake" {
  name          = "${var.lake_bucket_name}-${var.project_id}"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Iceberg gerencia suas próprias versões via snapshots (expire_snapshots).
  # Regra de ciclo de vida só para arquivos temporários de compactação.
  lifecycle_rule {
    action { type = "Delete" }
    condition {
      matches_prefix = ["_temp/"]
      age            = 7
    }
  }

  depends_on = [google_project_service.apis]
}

# ─────────────────────────────────────────────────────────────
# Bucket para o DuckDB do anomaly explainer (montado via GCS
# volume no Cloud Run v2 — sem estado efêmero no container).
# ─────────────────────────────────────────────────────────────
resource "google_storage_bucket" "anomaly_db" {
  name          = "pulso-anomaly-db-${var.project_id}"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  depends_on = [google_project_service.apis]
}
