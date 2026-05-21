# Contratos de dados (data contracts)

Schemas Avro que governam o barramento de eventos. **São a fonte da verdade dos schemas** — não há JSON livre nos tópicos Kafka.

## Por que aqui, versionados em git

Um schema é um **contrato explícito** entre produtor e consumidor (ver `[[conceitos/data-contracts]]` e `[[conceitos/schema-registry]]` na wiki). Versionar `.avsc` em git + validar compatibilidade no CI **desloca a quebra para o lado esquerdo** (shift-left): um PR que quebra um consumidor é bloqueado antes do merge, não descoberto em produção.

## Schemas

| Arquivo | Tópico | Descrição |
|---|---|---|
| `trade.avsc` | `trades.raw` | Trade individual (append-only, fonte da verdade). |
| `orderbook_delta.avsc` | `orderbook.delta` | Delta incremental do order book (com `update_id` para detecção de gap). |
| `candle.avsc` | `candles.m1` / `candles.m5` / `candles.h1` | Candle OHLCV agregado pelo ksqlDB. |

## Política de compatibilidade

- **BACKWARD** por subject (default): consumer novo lê dados antigos. Permite adicionar campo opcional ou remover campo.
- Campos novos **sempre com `default`** (Avro) — requisito para backward compatibility.
- **Nunca** mudar tipo de forma incompatível nem reusar semântica de um campo.

## Enforcement

`make schema-check` (e o job de CI `schema-compat`) roda `tools/check_schema_compat.py`, que valida cada `.avsc` deste diretório contra a última versão registrada no Schema Registry, conforme a política BACKWARD. Quebra → CI falha.
