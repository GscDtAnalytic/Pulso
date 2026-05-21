# ksqlDB — stream processing (Marco 2)

Agregações em **tempo real** sobre `trades.raw`: candles OHLCV, volatilidade e DLQ de
late data. Tudo por **event time** (princípio não-negociável #1). SQL versionado aqui;
testes de topologia em `tests/`.

## Arquivos

| Arquivo | O que cria | Destino |
|---|---|---|
| `00_streams.sql` | `trades_stream` sobre `trades.raw` (event-time) | — |
| `10_candles.sql` | `candles_m1` / `candles_m5` / `candles_h1` | tópicos `candles.m1/m5/h1` |
| `20_volatility.sql` | `volatility_5m` (janela HOPPING) | só pull query |
| `30_dlq.sql` | `trades_late` (late data) | tópico `trades.raw.dlq` |

A ordem dos números é a ordem de aplicação (cada um depende do anterior).

## Decisões de desenho

- **Candles = `EMIT FINAL`.** O tópico `candles.m1/m5/h1` carrega **só janelas seladas**:
  uma emissão por janela, depois do *grace period* (`is_final` sempre `true`). Estado
  live de janela aberta **não** vai para o tópico — é servido por **pull query** na
  TABLE materializada.
- **`symbol` não está no value do candle.** É a chave Kafka da mensagem (group key =
  message key). `contracts/candle.avsc` não tem o campo de propósito — difere de
  `trade.avsc` porque o candle é produzido pelo ksqlDB. `AS_VALUE(symbol)` não funciona
  numa TABLE agregada ("Logical and Physical schemas do not match").
- **Casing:** todo alias leva crases (`` `open` ``, `` `interval` ``…). Sem crases, o
  ksqlDB registra o campo em MAIÚSCULO no Avro e o schema diverge de `candle.avsc` —
  quebraria "schema é contrato".
- **Volatilidade não é tópico do barramento.** A lista canônica de tópicos (CLAUDE.md) é
  fixa. `volatility_5m` é materializada só para pull query.
- **Grace por intervalo:** m1 = 10s, m5 = 30s, h1 = 60s. Valores iniciais conservadores
  — calibrar pelo p99 do skew (`ingest_time - event_time`) medido no producer.

### Ressalva honesta: `open`/`close` por offset, não por event-time

`open`/`close` usam `EARLIEST_BY_OFFSET`/`LATEST_BY_OFFSET` — ordem de **offset** dentro
da janela. O ksqlDB não tem "first/last by event-time"; o *grace period* cobre o
reordenamento antes do fechamento. Mesmo trade-off da nota sobre Flink no
`ARCHITECTURE_PROPOSAL.md`. `high`/`low`/`volume`/`vwap` são comutativos — imunes a
ordem. `vwap = sum(price*qty)/sum(qty)`; `sum(qty)` nunca é 0 numa janela emitida
(`quantity > 0` e a janela só fecha com ≥1 trade).

## Aplicar na stack local

```bash
make up          # sobe Redpanda + ksqlDB + ...
make ksql-apply  # aplica ksqldb/[0-9]*.sql no ksqldb em :8088
```

## Pull queries (estado live)

Janela **aberta** não vai para tópico; consulte a TABLE materializada direto:

```sql
-- candle m1 corrente de um símbolo (estado parcial, ainda não selado)
SELECT * FROM candles_m1 WHERE symbol = 'BTC-USD';

-- volatilidade de 5 min mais recente
SELECT * FROM volatility_5m WHERE symbol = 'BTC-USD';
```

Via REST:

```bash
curl -s http://localhost:8088/query \
  -H 'Content-Type: application/vnd.ksql.v1+json' \
  -d '{"ksql":"SELECT * FROM candles_m1 WHERE symbol='\''BTC-USD'\'';"}'
```

## Testes de topologia

`make ksql-test` roda o `ksql-test-runner` offline (sem broker, sem Schema Registry
real) dentro da imagem do ksqldb-server. Casos em `tests/<caso>/`:
`sources.txt` (quais `.sql` de produção concatenar) + `input.json` + `expected.json`.

> **Limitação conhecida.** O `ksql-test-runner` standalone (0.29) **não dispara a
> emissão final por avanço de stream-time** — uma TABLE com `EMIT FINAL` nunca emite no
> teste. O runner (`tests/run.sh`) troca `EMIT FINAL` → `EMIT CHANGES`: o changelog
> cumulativo é exposto e o **último registro de cada janela é idêntico ao candle
> selado**. Os testes cobrem a agregação (OHLC/VWAP) e o schema/casing — o contrato.
> O *selamento* (emitir uma vez, pós-grace) é validado ao vivo com `make ksql-apply`.

`tests/test_ksqldb_assets.py` (no `pytest`/`make check`, sem docker) garante a
coerência dos arquivos — todo tópico esperado num `expected.json` é criado por algum
statement das suas `sources.txt`.
