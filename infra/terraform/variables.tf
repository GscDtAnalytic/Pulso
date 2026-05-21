variable "project_id" {
  description = "ID do projeto GCP."
  type        = string
}

variable "region" {
  description = "Região GCP para todos os recursos."
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Nome do ambiente (prod | staging)."
  type        = string
  default     = "prod"
}

variable "lake_bucket_name" {
  description = "Nome base do bucket GCS do lakehouse Iceberg (sufixo -<project_id> adicionado)."
  type        = string
  default     = "pulso-lakehouse"
}

variable "image_tag" {
  description = "Tag da imagem Docker no Artifact Registry (SHA do commit em CI)."
  type        = string
  default     = "latest"
}

variable "anthropic_api_key" {
  description = "Chave de API Anthropic para o LLM explainer (Marco 7)."
  type        = string
  sensitive   = true
  default     = ""
}

variable "alert_email" {
  description = "E-mail que recebe os alertas de SLO via Cloud Monitoring. Vazio = sem canal de notificação."
  type        = string
  default     = ""
}
