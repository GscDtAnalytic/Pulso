# Runbook — Reprocessamento Kappa

Guia operacional para reprocessar o lake a partir do log de eventos. O princípio é simples: o log (`trades.raw`, `candles.m1/m5/h1`) é a fonte da verdade. Corrigir o lake significa replay com nova lógica, não edição direta dos dados.

---

## Quando reprocessar

| Situação | O que muda | Seção |
|---|---|---|
| Bug na lógica de decode/serialização | Sink pipeline | [Replay do sink](#replay-do-sink) |
| Schema Iceberg evoluiu (campo novo) | Tabela Iceberg + sink | [Migração de schema](#migração-de-schema-iceberg) |
| Lógica de agregação do ksqlDB corrigida | Topologia ksqlDB + candles | [Migração ksqlDB](#migração-de-topologia-ksqldb) |
| Backfill de período histórico específico | Sink (intervalo de tempo) | [Backfill por timestamp](#backfill-por-timestamp) |
| Auditoria / reproducibilidade de análise | Somente leitura | [Backtest](#backtest-reproduzível) |

---

## Replay do sink

O sink é idempotente: replay do mesmo evento é no-op. Não precisa apagar dados antes.

```bash
# 1. Confirme que a stack está rodando
make up

# 2. (Opcional) Verifique o que existe no lake antes do replay
make backtest            # lista snapshots de silver.candles
uv run python services/backtest.py --list-snapshots --table bronze.trades

# 3. Execute o replay desde o início
make replay
# Equivalente a:
uv run python services/kappa_replay.py --from-beginning

# 4. Acompanhe o progresso via logs
#    O CLI imprime: received / inserted / skipped a cada batch

# 5. Após o replay, verifique o estado do lake
uv run python services/backtest.py --as-of $(date -u +%Y-%m-%dT%H:%M:%SZ)
make freshness-check
```

### Opções do CLI

```
--from-beginning           # replay desde offset 0 (todos os eventos do log)
--from-timestamp ISO       # replay desde o primeiro evento >= o timestamp
--pipeline trades          # só bronze.trades
--pipeline candles         # só silver.candles
--pipeline all             # ambos (default)
--consumer-group GROUP     # consumer group fixo (debug/idempotência entre runs)
--dry-run                  # mostra o que seria feito sem escrever
```

### O que o replay faz

1. Registra o HWM (high-water mark) de cada partição **antes** de consumir.
2. Cria um consumer group descartável (`pulso-kappa-replay-<timestamp>`).
3. Lê mensagens a partir do ponto escolhido e escreve via `IcebergSink` (MERGE idempotente).
4. Para quando alcança o HWM registrado no início — não compete com escrita live.

### Monitoramento durante o replay

```bash
# Consumer lag do grupo de replay (substitua <TIMESTAMP> pelo sufixo do grupo)
docker compose exec redpanda rpk group describe pulso-kappa-replay-<TIMESTAMP>

# Métricas do Prometheus (se o replay for longo, `make sink` pode estar no ar)
curl -s localhost:8002/metrics | grep pulso_records_written
```

---

## Backfill por timestamp

Para reprocessar apenas um período histórico:

```bash
# Replay desde 2026-05-01T00:00:00Z (só eventos >= essa data)
uv run python services/kappa_replay.py \
    --from-timestamp 2026-05-01T00:00:00Z \
    --pipeline trades
```

O MERGE garante que eventos anteriores ao timestamp, já presentes no lake, não sejam duplicados mesmo que o Kafka devolva mensagens antigas.

---

## Migração de schema Iceberg

Quando o schema de uma tabela evolui (campo novo com `default`):

```bash
# 1. Aplique a evolução de schema via PyIceberg ou Trino
#    Exemplo: adicionar campo 'source_ip' com default null em bronze.trades
docker compose exec trino trino \
    --execute "ALTER TABLE pulso.bronze.trades ADD COLUMN source_ip VARCHAR"

# 2. Confirme a compatibilidade do schema Avro (BACKWARD)
make schema-check-offline

# 3. Faça replay: o novo campo virá como null para eventos antigos (default)
make replay

# 4. Verifique integridade após replay
uv run python services/backtest.py \
    --as-of $(date -u +%Y-%m-%dT%H:%M:%SZ) \
    --table bronze.trades
```

---

## Migração de topologia ksqlDB

Quando a lógica de agregação de candles muda (ex.: nova fórmula de VWAP):

```bash
# 1. Versione as queries antigas (renomeie com sufixo _v1)
cp ksqldb/10_candles.sql ksqldb/10_candles_v1.sql

# 2. Edite a nova lógica em ksqldb/10_candles.sql

# 3. Pare o ksqlDB consumer antigo (o sink de candles)
#    O Kafka retém o log — nenhum evento é perdido.

# 4. Drop dos streams/tabelas antigos no ksqlDB
docker compose exec ksqldb ksql http://localhost:8088 \
    --execute "DROP TABLE IF EXISTS CANDLES_M1 DELETE TOPIC;"

# 5. Aplique a nova topologia
make ksql-apply

# 6. Aguarde o ksqlDB reprocessar 'trades.raw' (event-time, from beginning)
#    O ksqlDB usa um novo consumer group interno e publica candles re-agregados
#    nos tópicos 'candles.m1/m5/h1'.

# 7. Replay do sink de candles para atualizar o Iceberg
uv run python services/kappa_replay.py --from-beginning --pipeline candles

# 8. Reconstrua as marts dbt
make dbt-build

# 9. Verifique qualidade
make soda-check
```

### Atenção: janela de inconsistência

Entre o drop do stream antigo (passo 4) e o sink do ksqlDB novo (passo 7), o lake pode estar desatualizado. Para zero-downtime, use consumer groups versionados:

```bash
# Nova topologia com sufixo _v2, sem dropar a antiga
# Somente após o novo grupo estar caught up, redirecione o serving
```

---

## Backtest reproduzível

Consulte o estado exato do lake em qualquer ponto histórico:

```bash
# Listar todos os snapshots disponíveis
uv run python services/backtest.py --list-snapshots
uv run python services/backtest.py --list-snapshots --table bronze.trades

# Backtest em data específica (ex: ontem às 12h)
uv run python services/backtest.py --as-of 2026-05-20T12:00:00Z

# Backtest em snapshot específico (ID do --list-snapshots)
uv run python services/backtest.py --snapshot-id 1234567890123456789

# Backtest sobre bronze.trades
uv run python services/backtest.py --as-of 2026-05-20T12:00:00Z --table bronze.trades
```

O backtest verifica automaticamente os invariantes:
- `bronze.trades`: `price > 0`, `quantity > 0`
- `silver.candles`: `low ≤ open,close ≤ high`, `low ≤ high`

Exit 0 = dados consistentes. Exit 1 = invariante violada (fail-loud).

### Reprodutibilidade comprovada

```bash
# Rodar duas vezes com o mesmo --as-of deve dar resultado idêntico
uv run python services/backtest.py --as-of 2026-05-20T12:00:00Z 2>&1 | grep total
uv run python services/backtest.py --as-of 2026-05-20T12:00:00Z 2>&1 | grep total
# Saídas idênticas — prova que o lake é imutável para aquele snapshot
```

---

## Rollback via time-travel

Se um replay corrompeu dados (ex.: bug na nova lógica), reverta para o snapshot anterior:

```bash
# 1. Liste snapshots para encontrar o ponto bom
uv run python services/backtest.py --list-snapshots --table bronze.trades

# 2. Verifique que o snapshot anterior tem os dados corretos
uv run python services/backtest.py --snapshot-id <SNAPSHOT_ID_BOM>

# 3. Rollback via PyIceberg (reverte para o snapshot escolhido)
uv run python - <<'EOF'
from pulso_infra import get_settings
from pulso_storage.catalog import build_catalog

settings = get_settings()
catalog = build_catalog(settings)
table = catalog.load_table("bronze.trades")
table.manage_snapshots().rollback_to_snapshot(<SNAPSHOT_ID_BOM>).commit()
print("Rollback concluído.")
EOF

# 4. Verifique o estado após rollback
uv run python services/backtest.py --as-of $(date -u +%Y-%m-%dT%H:%M:%SZ) --table bronze.trades
```

> **Nota:** rollback não apaga snapshots posteriores — eles continuam existindo no histórico.
> Para libertar espaço, rode `make iceberg-maintain` após confirmar que o rollback está correto.

---

## Checklist de reprocessamento

```
[ ] Stack rodando (make up)
[ ] Backup mental: anotei o snapshot ID atual antes de replay
[ ] dry-run executado e comportamento esperado confirmado
[ ] Replay executado (make replay ou CLI com flags específicas)
[ ] Consumer lag do grupo de replay zerou (rpk group describe)
[ ] Backtest verificou invariantes após replay (exit 0)
[ ] Freshness SLO OK (make freshness-check)
[ ] dbt-build rodou sem erros se marts foram afetadas (make dbt-build)
[ ] Soda checks passaram (make soda-check)
[ ] Grupo de replay descartado (não aparece em rpk group list)
```
