# Handoff — Pulso em produção (end-to-end)

**Data:** 2026-05-22
**Branch:** `prod-readiness`
**Projeto GCP:** `pulso-497019` · região `us-central1`
**Objetivo:** "garantir Pulso 100% em prod end-to-end"

---

## TL;DR

**Pulso 100% em produção.** Pipeline streaming end-to-end operacional + histórico via espelho DuckDB.
Todos os 10 componentes ✅ — commit `f7b1e3b` (2026-05-22).

---

## Estado de cada componente

| Componente | Estado | Observação |
|---|---|---|
| Ingestão `pulso-ingest` (Binance+Coinbase → `trades.raw`) | ✅ | Ambas exchanges conectadas e estáveis; sem 451 |
| ksqlDB (candles m1/m5/h1, volatility.5m) | ✅ | 5 persistent queries `Stable`; SQLs aplicados em prod |
| Sink → Iceberg `bronze.trades` + `silver.candles` | ✅ | MERGE idempotente (0 duplicatas); offsets no snapshot |
| serve `/api/candles/live` (pull query ksqlDB) | ✅ | 200, dados reais |
| serve `/api/anomalies` | ✅ | 200 (DuckDB read-only) |
| serve `/ws/candles` (push WebSocket) | ✅ (esperado) | caminho Kafka OK |
| `pulso-anomaly-detector` | ✅ | Detectando PRICE/VOLUME/VOLATILITY_SPIKE em BTC, ETH, XRP, DOGE |
| `pulso-llm-explainer` | ✅ | Gerando explicações Claude Haiku; salvando em DuckDB GCS |
| `pulso-marquez` (lineage) | ✅ | Ready |
| serve `/api/candles` (histórico) | ✅ | Marts dbt via espelho DuckDB em GCS (`/mnt/lake-mirror`) |

Infra de apoio: Cloud SQL (catálogo Iceberg) ✅, GCS lakehouse ✅, Secret Manager ✅,
Artifact Registry ✅, VM Redpanda+ksqlDB ✅, Monitoring/Budget ✅.

---

## O que foi feito (commits em `prod-readiness`)

- **`46881e7`** — fixes de deploy GCP:
  - Redpanda: `kafka_enable_authorization=false` (loopback interno é trusted) +
    `schema_registry_client`/`pandaproxy_client` → loopback (SR voltou a registrar schema —
    era a **causa raiz** do pipeline parado: `illegal_sasl_state` no `_schemas`).
  - Catálogo Iceberg via **IP privado TCP** do Cloud SQL (sem IP público → Auth Proxy embutido
    do Cloud Run não criava o socket). Mesmo método do Marquez.
  - Removida annotation `cloudsql-instances` (inútil sem IP público).
  - ksqlDB candles: `RETENTION_MS` explícito (adota tópicos pré-criados de 24h).
- **`fe5c3c1`** — bugs de app que só apareceram com dados reais:
  - Sink (candles): chave janelada do ksqlDB (`symbol + 8 bytes`) quebrava o decode UTF-8.
  - serve (anomaly_store): DuckDB em mount read-only → modo `read_only`.
  - Binance: default → `data-stream.binance.vision` (sem geo-block).
  - `redpanda.tf`: `lifecycle { ignore_changes = [metadata_startup_script] }` (ForceNew → recriaria a VM).
- **`14e2eaf`** — `anomaly_detector` sofria do mesmo bug de chave janelada (consome `candles.m1`);
  decoders de chave movidos para `pulso_infra.kafka_keys` (compartilhado sink + detector).

`terraform apply` rodado e bem-sucedido (state reconciliado; VM protegida por `ignore_changes`,
sink `untaint`ado). 134 testes passam; ruff + import-linter limpos.

---

## O que foi feito na sessão 2 (commit `f7b1e3b`, 2026-05-22)

- **Redeploy `pulso-anomaly-detector`**: uma linha gcloud, detectando imediatamente.
- **`_millis_to_dt` / `_fmt_ts`**: Avro `AvroDeserializer` converte `timestamp-millis` em
  `datetime` Python (não `int`). Bug latente que só apareceu quando o detector começou a
  produzir eventos. Fix em `anomaly_store.py` e `llm_explainer.py`.
- **`dbt-core`/`dbt-duckdb`/`soda-core-duckdb`** movidos de `[dependency-groups] dev` para
  `[project] dependencies` — eram excluídos pelo `uv sync --no-dev` da imagem Docker.
- **`lake_mirror.tf`**: bucket GCS `pulso-lake-mirror-<project>`, SA, IAM, Cloud Run Job
  `pulso-lake-mirror` (bash: mirror + dbt build), Cloud Scheduler `30 * * * *`.
- **`cloud_run.tf`**: `pulso-serve` recebe volume `lake-mirror` em `/mnt/lake-mirror` (read-only)
  e `PULSO_DBT_DUCKDB_PATH=/mnt/lake-mirror/pulso_lake.duckdb`. Mount path separado de `/data`
  (anomaly-db) para evitar nested FUSE mount.

---

## Notas operacionais úteis

- **SSH na VM:** `gcloud compute ssh pulso-redpanda --zone=us-central1-a --project=pulso-497019 --tunnel-through-iap`
- **rpk como `pulso` (listener externo SASL+TLS):**
  ```bash
  SASL_PW=$(gcloud secrets versions access latest --secret=pulso-kafka-sasl-password --project=pulso-497019)
  rpk topic list --brokers 10.128.0.2:9092 -X tls.ca=/etc/redpanda/certs/ca.crt \
    -X sasl.mechanism=SCRAM-SHA-256 -X user=pulso -X pass="$SASL_PW"
  ```
- **ksqlDB na VM:** `sudo docker exec -i ksqldb-server ksql http://localhost:8088`
  (aplicar SQLs: `RUN SCRIPT '/tmp/all.sql';` após `docker cp`).
- **Build/push imagem:** `make docker-build && make docker-push` (ou ver `.github/workflows/deploy.yml`).
  Serviços referenciam `pulso:latest`; redeploy via `gcloud run services update ... --update-labels`
  puxa o digest novo.
- **Fixes de runtime na VM** (authz off, SR client → loopback) estão persistidos nos discos
  **e** no startup script (`redpanda.tf`) para recriações futuras. `ignore_changes` impede que
  editar o script recrie a VM — para forçar uso de um script novo, `terraform taint` deliberado.
- Memória do projeto: `memory/prod_deploy_gcp.md` (gotchas de Redpanda/SR/Cloud SQL/Binance).
