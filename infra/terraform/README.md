# Terraform — GCP (Marco 8)

Provisiona toda a infraestrutura GCP do Pulso: GCS, Artifact Registry, Cloud SQL,
Secret Manager, IAM e Cloud Run.

## Recursos criados

| Recurso | Finalidade |
|---|---|
| `google_storage_bucket.lake` | Lakehouse Iceberg (bronze/silver/gold) |
| `google_storage_bucket.anomaly_db` | DuckDB de anomalias (montado via GCS volume) |
| `google_artifact_registry_repository.pulso` | Imagens Docker dos serviços |
| `google_sql_database_instance.iceberg_catalog` | Postgres para catálogo JDBC Iceberg |
| `google_cloud_run_v2_service.*` | 5 serviços Cloud Run (ver abaixo) |
| `google_secret_manager_secret.*` | Credenciais Kafka, Anthropic, URI do catálogo |
| `google_service_account.*` | SA de menor privilégio por serviço |
| `google_iam_workload_identity_pool*` | CI/CD sem chave de SA de longa duração |

## Serviços Cloud Run

| Serviço | Módulo | min/max | Visibilidade |
|---|---|---|---|
| `pulso-ingest` | `python -m pulso_ingest` | 1/1 | interna |
| `pulso-sink` | `python -m pulso_storage` | 1/1 | interna |
| `pulso-anomaly-detector` | `python services/anomaly_detector.py` | 1/1 | interna |
| `pulso-llm-explainer` | `python services/llm_explainer.py` | 1/1 | interna |
| `pulso-serve` | `python -m pulso_serve` | 0/5 | pública |

## Pré-requisitos

1. [Terraform >= 1.8](https://developer.hashicorp.com/terraform/install)
2. `gcloud auth application-default login`
3. Projeto GCP criado e faturamento habilitado

## Uso rápido

```bash
cp terraform.tfvars.example terraform.tfvars
# edite terraform.tfvars

make tf-init    # terraform init
make tf-plan    # terraform plan
make tf-apply   # terraform apply
```

## Configurar GitHub Actions (CI/CD)

Após o `apply`, adicione as variáveis de repositório no GitHub:

```bash
WIF_PROVIDER=$(terraform output -raw wif_provider)
DEPLOY_SA=$(terraform output -raw deploy_sa_email)
AR_URL=$(terraform output -raw artifact_registry_url)

gh variable set WIF_PROVIDER   --body "$WIF_PROVIDER"
gh variable set DEPLOY_SA      --body "$DEPLOY_SA"
gh variable set ARTIFACT_URL   --body "$AR_URL"
gh variable set GCP_PROJECT_ID --body "$(terraform output -raw project_id 2>/dev/null || echo $TF_VAR_project_id)"
gh variable set GCP_REGION     --body "us-central1"
```

## Conectar ao Cloud SQL (local → prod)

```bash
# Cloud SQL Auth Proxy
cloud-sql-proxy --port 5433 $(terraform output -raw cloud_sql_connection_name)

# Em outro terminal:
psql "host=127.0.0.1 port=5433 user=pulso dbname=pulso"
```

## Notas de custo

Ver `infra/COST_ANALYSIS.md`.
