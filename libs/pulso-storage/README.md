# pulso-storage — lakehouse Iceberg (Marco 3)

Sink **idempotente** Kafka → Apache Iceberg. Materializa o barramento em duas
camadas medallion sobre object storage (MinIO em dev, GCS em prod), com
particionamento, manutenção e time-travel para backtest reproduzível.

```
trades.raw            ──┐
                        ├─ sink (MERGE por chave) ─▶ bronze.trades   (Iceberg)
candles.m1/m5/h1      ──┘                          ─▶ silver.candles  (Iceberg)
```

## Por que PyIceberg (e não Kafka Connect)

As duas portas estavam abertas no desenho. Escolhido **PyIceberg**:
mantém o sink em Python (mesma stack do producer do Marco 1), testável 100% offline
no `pytest` (catálogo sqlite + warehouse local), sem um conector Java para operar.
O custo — escala de escrita — está anotado abaixo como caminho de evolução, não
varrido para baixo do tapete.

## Exactly-once: o desenho

Dois mecanismos que se reforçam (`sink.py`):

1. **MERGE pela chave de negócio.** `Table.upsert(join_cols=…, when_matched_update_all=False)`:
   linha que já existe → não faz nada; linha nova → insere. Reentregar um batch
   (consumer reinicia, exchange manda o trade duas vezes) insere zero. Chave:
   trades = `(exchange, symbol, trade_id)` — `trade_id` é local à exchange, não
   global; candles = `(symbol, interval, window_start)`.
2. **Offset no snapshot.** Os offsets Kafka consumidos vão para as
   `snapshot_properties` do **mesmo commit Iceberg** que grava os dados — append e
   avanço de offset são um commit atômico. No restart, o sink lê os offsets do
   snapshot e dá `seek`: nada de replay desde o início do log; o pouco que
   reprocessar, o MERGE deduplica.

`enable.auto.commit=false` de propósito: a verdade do progresso mora no lake, não
no offset commitado do Kafka.

## Camadas e particionamento

| Tabela | Origem | Partição | Espelha |
|---|---|---|---|
| `bronze.trades` | `trades.raw` | `day(event_time)` + `symbol` | `contracts/trade.avsc` |
| `silver.candles` | `candles.m1/m5/h1` | `day(window_start)` + `interval` | `contracts/candle.avsc` + `symbol` |

`symbol` não está no value do candle (é a chave Kafka — ver `ksqldb/README.md`); o
sink o injeta a partir da chave. Particionar por `day + symbol/interval` poda a
varredura do anti-join do MERGE e do time-travel.

## Catálogo

Catálogo **SQL** Iceberg sobre URI SQLAlchemy:

- **dev/prod** — Postgres do `docker-compose`. É o mesmo catálogo que o Trino lerá
  no Marco 4 (conector `iceberg` jdbc): multi-engine sobre um dado só.
- **testes** — sqlite + warehouse `file://`, sem broker nem MinIO.

## Uso

```bash
make up                 # sobe Redpanda + MinIO + Postgres + ...
make sink               # roda o sink (trades + candles) — /metrics em :8002
make iceberg-maintain   # expire_snapshots nas tabelas do lake

uv run python -m pulso_storage --pipeline trades   # só um pipeline
uv run python -m pulso_storage maintain --retain-hours 24
```

Time-travel (backtest reproduzível) — `pulso_storage.timetravel`:

```python
from pulso_storage import build_catalog, TRADES
from pulso_storage.timetravel import list_snapshots, scan_as_of, to_duckdb
from pulso_infra import get_settings

table = build_catalog(get_settings()).load_table(TRADES.identifier)
snap = list_snapshots(table)[0]                       # versão mais antiga
con = to_duckdb(scan_as_of(table, snapshot_id=snap.snapshot_id))
con.sql("SELECT symbol, count(*) FROM snapshot GROUP BY 1").show()
```

## Ressalvas honestas

- **Escala de escrita.** O `upsert` do PyIceberg varre as partições tocadas pelo
  batch para o anti-join. `day + symbol` poda essa varredura e batches pequenos a
  mantêm barata; em volume alto, o MERGE do Trino ou o Kafka Connect Iceberg sink
  escalam melhor — caminho de evolução, mesmo tom da nota Flink.
- **Compactação.** `expire_snapshots` roda aqui (PyIceberg). `rewrite_data_files`
  (bin-pack) ainda não é exposto pelo PyIceberg 0.11 — roda pelo Trino
  (`ALTER TABLE … EXECUTE optimize`) quando ele entrar como engine no Marco 4.

## Observabilidade

`/metrics` em `:8002` (Prometheus): linhas escritas, duplicatas ignoradas pelo
MERGE, consumer lag (métrica #1 de streaming), freshness `now − max(event_time)`,
latência de commit. Fail-loud: lag e freshness são de primeira classe.
