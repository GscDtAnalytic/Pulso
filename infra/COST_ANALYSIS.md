# Análise de Custo — GCP (Marco 8)

Estimativas mensais para o ambiente de produção do Pulso.
Preços em USD; região `us-central1`. Último check: maio 2026.

## Resumo executivo

| Cenário | Custo/mês |
|---|---|
| Mínimo (portfolio, baixo tráfego) | ~$35–45 |
| Operação normal (tráfego moderado) | ~$60–80 |

---

## Detalhamento por serviço

### Cloud Run — Serviços de streaming (workers contínuos)

Os 4 workers (ingest, sink, anomaly-detector, llm-explainer) rodam com `min-instances=1`.
Cloud Run cobra CPU/memória ociosa a **20% da tarifa normal** para instâncias mínimas.

**Por worker** (0.5 vCPU, 512 MiB, idle 80% do tempo):

| Componente | Cálculo | USD/mês |
|---|---|---|
| CPU (ociosa) | 0.5 × $0.0000240/s × 0.2 × 2 592 000 s | $6.22 |
| Memória (ociosa) | 0.5 GiB × $0.0000025/GiB·s × 0.2 × 2 592 000 s | $0.65 |
| **Worker subtotal** | | **~$6.90/mês** |

4 workers × $6.90 = **~$27.60/mês**

### Cloud Run — pulso-serve (FastAPI, escala a zero)

`min-instances=0`: cobra só por requisição. Estimativa para 10 000 req/dia:

| Componente | Cálculo | USD/mês |
|---|---|---|
| Requisições | 300 000 × $0.40/1M | $0.12 |
| CPU (1 vCPU, 200ms/req) | 300 000 × 0.2s × $0.0000240 | $1.44 |
| Memória (512 MiB, 200ms/req) | 300 000 × 0.5 × 0.2s × $0.0000025 | $0.075 |
| **Serve subtotal** | | **~$1.60/mês** |

### Cloud SQL — db-f1-micro (catálogo Iceberg)

| Item | USD/mês |
|---|---|
| Instância db-f1-micro | $7.67 |
| Armazenamento SSD 10 GiB | $1.70 |
| Backup automático (7 dias) | ~$0.50 |
| **Cloud SQL subtotal** | **~$9.90/mês** |

### GCS — Lakehouse Iceberg

Estimativa para 10 GiB de dados Iceberg (bronze + silver + gold):

| Item | USD/mês |
|---|---|
| Armazenamento Standard (10 GiB) | $0.20 |
| Operações Class A (escrita) 100K/mês | $0.05 |
| Operações Class B (leitura) 500K/mês | $0.02 |
| Egress intra-região (Cloud Run → GCS) | $0.00 |
| **GCS subtotal** | **~$0.27/mês** |

> Egress entre Cloud Run e GCS na mesma região é gratuito.

### Artifact Registry

| Item | USD/mês |
|---|---|
| Armazenamento (<1 GiB, política keep-last-10) | $0.10 |
| **AR subtotal** | **~$0.10/mês** |

### Secret Manager

Até 6 secrets ativos = dentro da cota gratuita (primeiros 6 secrets/mês grátis, depois $0.06/10K acessos).
**~$0.00/mês**

---

## Total estimado

| Componente | USD/mês |
|---|---|
| Cloud Run workers (×4) | $27.60 |
| Cloud Run serve | $1.60 |
| Cloud SQL | $9.90 |
| GCS | $0.27 |
| Artifact Registry | $0.10 |
| Secret Manager | $0.00 |
| **Total** | **~$39.47/mês** |

---

## Comparação com alternativas

| Opção | Custo/mês | Trade-off |
|---|---|---|
| **GCP atual (este design)** | ~$40 | Serverless, escala automática, sem VMs para gerir |
| GCE e2-medium (VM única) | ~$25 | Gerenciamento manual, sem auto-scaling |
| GKE Autopilot | ~$60–100 | Mais overhead, melhor para escala multi-serviço |
| AWS equivalente (ECS + S3 + RDS) | ~$45 | Similar; GCS egress intra-região mais barato |

---

## Otimizações possíveis

1. **Suspender workers fora do horário de trading** (`min-instances=0` à noite):
   reduz custo dos workers em ~50% → economia de ~$13/mês.

2. **Cloud SQL → SQLite no GCS (catálogo Iceberg)**: elimina o db-f1-micro (~$10/mês),
   mas perde multi-engine (Trino não leria o mesmo catálogo sqlite).

3. **Cloud Run jobs** para o sink (em vez de serviço contínuo):
   adequado se a latência de ingestão for tolerante a microbatches de 1–5 min.

4. **Committed Use Discounts**: 1 ano de commit → ~17% de desconto nos workers.
