# RUNBOOK — Prontidão para produção

Plano de remediação dos achados da auditoria de produção (critério: wiki
`Data/wiki`, checklist dos 6 undercurrents). Itens em ordem de prioridade.

## Progresso

| # | Item | Severidade | Estado |
|---|------|-----------|--------|
| 1 | SPOF do broker (Redpanda/ksqlDB em VM única) | 🔴 bloqueador | ✅ feito |
| 2 | Sem plano de coleta de observabilidade em prod | 🔴 bloqueador | ✅ feito |
| 3 | Sem scheduler para jobs de manutenção em prod | 🔴 bloqueador | ✅ feito |
| 4 | State do Terraform local + não-gitignored | 🔴 bloqueador | ✅ feito |
| 5 | Kafka/SR/ksqlDB em texto claro (sem TLS) | 🟡 importante | ✅ feito |
| 6 | CI não testa dado nem compat de schema ao vivo | 🟡 importante | ✅ feito |
| 7 | Sem ambiente de staging / gate de aprovação | 🟡 importante | ✅ feito |
| 8 | Cloud SQL sem HA + IP público | 🟡 importante | ✅ feito |
| 9 | `pulso-serve` público sem authn/rate limiting | 🟡 importante | ✅ feito |
| 10 | Retenção de tópico de 24h limita replay | 🟡 importante | ✅ feito |
| 11 | Lineage (Marquez) não vai para produção | 🟡 importante | ✅ feito |

---

## §1 — Broker Redpanda + ksqlDB (resiliência da VM única)

**Decisão:** portfolio-grade — VM única, sem HA real, custo ~$0 extra.
Não é HA; é resiliência com RPO/RTO explícitos. Para HA real, migrar para
3 nós Redpanda ou Redpanda Cloud (ver `infra/terraform/redpanda.tf` topo).

### O que foi implementado (`redpanda.tf`)

- **Disco de dados dedicado** (`google_compute_disk.redpanda_data`, 50 GB
  pd-ssd) — Redpanda WAL/segments + state do ksqlDB vivem aqui, não no boot
  disk. Disco standalone → **sobrevive à destruição da VM**.
- **Snapshot diário** (`google_compute_resource_policy`, 04:00 UTC, retenção
  7 dias, incremental) anexado ao disco de dados.
- **`automatic_restart` + `on_host_maintenance = MIGRATE`** — o GCE reinicia
  a VM em crash do guest / falha de host e faz live-migration em manutenção.
- Boot disk reduzido a 20 GB pd-balanced (descartável; o startup script
  reinstala tudo). ksqlDB com `KSQL_KSQL_STREAMS_STATE_DIR` no disco de dados.

### RPO / RTO

| Cenário | RPO | RTO | Recuperação |
|---------|-----|-----|-------------|
| Crash do guest / falha de host | ~0 | 2–5 min | `automatic_restart` do GCE; disco de dados intacto |
| VM destruída (acidente / `terraform destroy` parcial) | ~0 | 5–10 min | `terraform apply` recria a VM e reanexa o disco de dados |
| Perda do disco de dados | ≤ 24 h | 10–20 min | Restaurar disco do último snapshot (abaixo) |
| Perda da zona inteira | ≤ 24 h | manual | Recriar em outra zona a partir do snapshot |

> Mitigação adicional: `bronze.trades` no Iceberg é a cópia durável dos
> trades; os tópicos Kafka já têm retenção de 24 h de qualquer forma.

### Restore do disco de dados a partir de snapshot

```bash
ZONE="us-central1-a"
# 1. Listar snapshots disponíveis
gcloud compute snapshots list --filter="name~pulso-redpanda" --sort-by=~creationTimestamp
# 2. Parar/remover a VM (se ainda existir) e o disco corrompido
gcloud compute instances delete pulso-redpanda --zone="$ZONE" --quiet
gcloud compute disks delete pulso-redpanda-data --zone="$ZONE" --quiet
# 3. Recriar o disco a partir do snapshot mais recente
gcloud compute disks create pulso-redpanda-data \
  --source-snapshot=<NOME_DO_SNAPSHOT> --type=pd-ssd --zone="$ZONE"
# 4. terraform apply — recria a VM e reanexa o disco restaurado
terraform apply
```

> Como o disco já está formatado e populado, o startup script apenas o
> remonta (`blkid` detecta o filesystem) — Redpanda e ksqlDB sobem com o
> estado anterior.

---

## §2 — Coleta de observabilidade em produção

**Problema:** os serviços expunham `/metrics`, mas nada raspava na nuvem;
as alerting rules de `governance/` nunca eram avaliadas.

### O que foi implementado

- **Sidecar GMP** (`cloud_run.tf`) — cada um dos 5 serviços Cloud Run ganhou
  um container `collector` (imagem `cloud-run-gmp-sidecar:1.2.0`) que raspa
  `localhost:8080/metrics` a cada 30s e envia ao Managed Service for
  Prometheus. `depends_on = ["app"]` garante a ordem de start/stop.
- **IAM** (`iam.tf`) — as 5 SAs ganharam `roles/monitoring.metricWriter`.
- **Alert policies** (`monitoring.tf`) — 4 políticas com condição PromQL
  nativa, espelhando `governance/slo.yml`: freshness, consumer lag, p99 da
  API e latência e2e de candles. Avaliadas pelo Cloud Monitoring.
- **APIs** — `monitoring.googleapis.com` habilitada em `main.tf`.

### Ação do operador

Defina `alert_email` no `terraform.tfvars` para receber os alertas por
e-mail (sem isso, as políticas existem mas não notificam):

```hcl
alert_email = "voce@exemplo.com"
```

Métricas ficam em **Cloud Monitoring → Metrics Explorer** (PromQL) e em
**Managed Prometheus**. Custo: GMP é por amostra ingerida — baixo neste
volume; o sidecar adiciona ~256Mi RAM por serviço.

---

## §3 — Jobs de manutenção agendados

**Problema:** `expire_snapshots`/compactação Iceberg eram alvos `make` sem
scheduler em prod → small-file explosion garantida com o tempo.

### O que foi implementado (`maintenance.tf`)

- **Cloud Run Job** `pulso-iceberg-maintain` — roda `python -m pulso_storage
  maintain`, reutiliza a SA do sink (já tem GCS + Cloud SQL + secrets).
- **Cloud Scheduler** `pulso-iceberg-maintain-daily` — dispara o job às
  05:00 UTC (depois do snapshot do disco Redpanda às 04:00), via OAuth com
  a SA `pulso-scheduler-sa` (`roles/run.invoker` só nesse job).
- `cloudscheduler.googleapis.com` habilitada em `main.tf`.

### Fora de escopo (por desenho)

`soda-check` e `lake-mirror` são bridges de dev (DuckDB/dbt-dev) e não rodam
em prod. Qualidade contínua em prod é tratada no item 6.

---

## §4 — State do Terraform remoto e seguro

**Problema:** backend local → `terraform.tfstate` (com `random_password` do
banco) em disco; `.terraform/` e `*.tfstate` não estavam no `.gitignore`.

### O que foi implementado

- **Backend GCS** (`versions.tf`) — `backend "gcs" {}` com config parcial;
  `bucket`/`prefix` vêm de `backend.hcl` (gitignored).
- **`.gitignore`** — adicionados `**/.terraform/`, `*.tfstate*`,
  `crash*.log`, `backend.hcl`.
- **`backend.hcl.example`** — template versionado.
- **`make tf-init`** — agora usa `-backend-config=backend.hcl`.

### Bootstrap único do bucket de state

O bucket precisa existir antes do `terraform init`:

```bash
PROJECT_ID="<seu-projeto>"
gcloud storage buckets create "gs://pulso-tfstate-$PROJECT_ID" \
  --project="$PROJECT_ID" --location=us-central1 \
  --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets update "gs://pulso-tfstate-$PROJECT_ID" --versioning

cp infra/terraform/backend.hcl.example infra/terraform/backend.hcl
# edite backend.hcl com o nome real do bucket, então:
make tf-init   # migra o state local (se houver) para o GCS
```

---

## §5 — TLS + SASL/SCRAM no barramento

**Problema:** Kafka (9092), Schema Registry (8081) e ksqlDB falavam texto
claro; sem autenticação. A wiki (`cloud-security`) exige TLS em trânsito.

### Desenho

| Caminho | Transporte |
|---------|-----------|
| Cliente Cloud Run → Redpanda (9092) | **TLS + SASL/SCRAM-SHA-256** |
| Cliente Cloud Run → Schema Registry (8081) | **TLS (HTTPS)** |
| ksqlDB → Redpanda / SR (mesma VM) | texto claro em **loopback** (29092/18081) — sem tráfego na rede |
| pulso-serve → ksqlDB REST (8088) | HTTP intra-VPC — risco residual aceito |

O broker expõe **dois listeners**: `external` (IP interno, TLS+SASL) e
`internal` (127.0.0.1, texto claro) — este só para o ksqlDB no mesmo host.

### O que foi implementado

- **PKI** (`tls.tf`) — CA self-signed + cert do servidor (SAN = IP interno),
  gerados no `apply`. Chaves no state (por isso o state é remoto, item 4).
- **Secrets** (`secrets.tf`) — CA cert, cert/chave do servidor, senha SASL;
  `schema_registry_url` agora é `https://`.
- **Broker** (`redpanda.tf`) — startup script busca certs/senha do Secret
  Manager, configura os 2 listeners + TLS, liga `enable_sasl`, cria o usuário
  SCRAM `pulso` (superuser). ksqlDB roda com `--network host` nos listeners
  loopback.
- **Clientes** — `pulso_infra` ganhou `kafka_security_config()` /
  `schema_registry_config()`; os 6 sites de cliente Kafka/SR
  (producer, sink, replay, anomaly, explainer, serve-WS) usam os helpers.
  Dev continua `PLAINTEXT` por default — zero impacto local.
- **Cloud Run** — `PULSO_KAFKA_SECURITY_PROTOCOL`/`_SASL_USERNAME` via
  `common_env`; senha SASL e CA cert (PEM) via secrets; IAM concede acesso.

### ⚠️ Validação no primeiro deploy

O startup script do broker (config TLS/SASL do Redpanda) **não é testável
offline**. No primeiro `terraform apply`, conferir em
`/var/log/redpanda-startup.log` na VM: listeners no ar, usuário `pulso`
criado, tópicos criados. Ajustar chaves de config se a versão do Redpanda
divergir. Os clientes Python e o Terraform já estão validados
(`make check` + `terraform validate`).

---

## §6 — CI testa dado e compat de schema ao vivo

**Problema:** o CI só fazia parse offline dos `.avsc`; e o teste de build dbt
**era silenciosamente pulado** (a venv usava Python 3.14, onde dbt-core não
importa) — ou seja, o dbt não era testado em lugar nenhum.

### O que foi implementado

- **`.python-version` = 3.12** — fixa o interpretador onde o dbt funciona. O
  teste `test_dbt_marts.py::test_dbt_build_succeeds` (que roda `dbt build` +
  testes contra um lake fixture) agora **executa** no CI (131 testes, 0 skip).
- **Compat BACKWARD ao vivo** — o job `schema-compat` sobe um Redpanda,
  registra os contratos de `origin/main` como baseline (`--baseline` novo em
  `check_schema_compat.py`) e checa os do PR contra ela. Quebra bloqueia merge.

---

## §7 — Staging e gate de aprovação

**Problema:** `deploy.yml` fazia deploy direto em prod no push para `main`,
sem aprovação.

### O que foi implementado

- **Gate de aprovação** — o job de deploy usa `environment: production`.
  Configure *required reviewers* em **Settings → Environments → production**:
  o deploy passa a pausar aguardando aprovação humana.
- **Cadeia dev → CI → prod** — o CI (lint + 131 testes + ksql + compat ao
  vivo + governança) é o portão de qualidade antes do deploy.

### Staging GCP (opcional)

A variável `environment` (`prod | staging`) já parametriza os nomes dos
recursos. Para um ambiente isolado: `terraform workspace new staging` +
`-var environment=staging`. Não é provisionado por default (custo).

---

## §8 — Cloud SQL endurecido

**Problema:** `db-f1-micro`, zona única, **IP público**.

### O que foi implementado (`cloud_sql.tf`)

- **Sem IP público** — `ipv4_enabled = false` + IP privado via Private
  Service Access (peering da VPC; `servicenetworking.googleapis.com`).
- **Point-in-time recovery** — `point_in_time_recovery_enabled` + 7 dias de
  retenção de WAL, sobre o backup diário já existente.
- **`deletion_protection = true`** — protege o catálogo de destroy acidental.

Decisão portfolio-grade: **sem HA regional** (zona única). Failover = restore
do backup/PITR. Para HA real, `availability_type = "REGIONAL"` (exige subir
o tier — custo).

---

## §9 — Rate limiting na API pública

**Problema:** `pulso-serve` público (`allUsers`) sem proteção contra abuso.

### O que foi implementado

- **slowapi** no app FastAPI (`apps/dashboard/api`) — `SlowAPIMiddleware` com
  limite global de **120 req/min por IP**. A API segue pública (é vitrine de
  portfólio); o limite contém abuso sem fechar o showcase.
- A chave de rate limit lê o **X-Forwarded-For** (IP real atrás do Cloud Run).
- WebSocket (`/ws/candles`) não é afetado (o middleware só intercepta HTTP).

---

## §10 — Retenção de tópico

**Problema:** todos os tópicos com retenção de 24h — se o sink ficar fora
>24h, `trades.raw` (fonte da verdade) é perdido.

### O que foi implementado (`redpanda.tf`)

- **Origem/eventos** (`trades.raw`, `trades.raw.dlq`, `orderbook.delta`,
  `events.anomaly*`) → **7 dias**. Janela de replay Kappa ampla.
- **Candles** (derivados, reconstrutíveis) → 24h.

---

## §11 — Lineage (Marquez) em produção

**Problema:** Marquez só existia no `docker-compose`; em prod a emissão
OpenLineage não tinha backend.

### O que foi implementado

- **Cloud Run `pulso-marquez`** (`marquez.tf`) — ingress interno, conecta ao
  Postgres pelo **IP privado** do Cloud SQL (banco `marquez` dedicado, mesma
  instância — sem custo de instância nova).
- **Wiring** — `PULSO_OPENLINEAGE_URL` aponta o sink e o producer para o
  serviço Marquez (`cloud_run.tf`).
- Custo: +1 Cloud Run interno (~$15/mês); o banco reusa a instância.

### ⚠️ Validação no primeiro deploy

A imagem `marquezproject/marquez` roda migrações Flyway no boot. Conferir os
logs do `pulso-marquez` no primeiro deploy (migração concluída, API na :5000).
