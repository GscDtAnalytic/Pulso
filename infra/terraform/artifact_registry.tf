resource "google_artifact_registry_repository" "pulso" {
  location      = var.region
  repository_id = "pulso"
  description   = "Imagens Docker dos serviços Pulso"
  format        = "DOCKER"

  cleanup_policies {
    id     = "keep-last-10"
    action = "KEEP"
    most_recent_versions {
      keep_count = 10
    }
  }

  depends_on = [google_project_service.apis]
}
