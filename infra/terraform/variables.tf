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

variable "kafka_bootstrap" {
  description = "Endereço do broker Kafka/Redpanda Cloud (ex: seed-xxx.redpanda.cloud:9092)."
  type        = string
  sensitive   = true
  default     = ""
}

variable "schema_registry_url" {
  description = "URL do Schema Registry (Redpanda Cloud ou self-hosted)."
  type        = string
  sensitive   = true
  default     = ""
}

variable "anthropic_api_key" {
  description = "Chave de API Anthropic para o LLM explainer (Marco 7)."
  type        = string
  sensitive   = true
  default     = ""
}
