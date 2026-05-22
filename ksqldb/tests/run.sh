#!/usr/bin/env bash
# Testes de topologia ksqlDB — offline, via ksql-test-runner (sem broker, sem SR real).
#
# Para cada caso em ksqldb/tests/<caso>/ (com sources.txt + input.json + expected.json):
#   1. concatena os .sql de producao listados em sources.txt (uma fonte da verdade —
#      o teste roda o MESMO SQL que vai para producao, sem copia paralela);
#   2. remove linhas `SET ...` — sao config de sessao do ksqlDB; o test-runner nao as
#      executa e aborta com "Statement not executable" se encontra-las;
#   3. troca `EMIT FINAL` -> `EMIT CHANGES`. LIMITACAO CONHECIDA do ksql-test-runner
#      standalone (0.29): ele NAO dispara a emissao final por avanco de stream-time,
#      entao uma TABLE com EMIT FINAL nunca produz registro no teste. EMIT CHANGES
#      expoe o changelog cumulativo; o ULTIMO registro de cada janela e identico ao
#      candle selado que EMIT FINAL emitiria. O teste cobre, assim, a agregacao
#      (OHLC/VWAP/volume/trade_count) e o schema/casing dos campos — o contrato.
#      O comportamento de selamento (emitir UMA vez, pos-grace) e validado ao vivo
#      com `make ksql-apply` na stack do docker-compose.
#   4. roda o ksql-test-runner dentro da imagem do ksqldb-server (o binario ja existe
#      em /usr/bin/ksql-test-runner — nao precisa baixar o ksqldb-cli).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
KSQLDIR="$ROOT/ksqldb"
TESTS_DIR="$KSQLDIR/tests"
IMAGE="${KSQLDB_IMAGE:-confluentinc/ksqldb-server:0.29.0}"

work="$(mktemp -d)"
chmod 777 "$work"   # container ksqldb roda como non-root; precisa de leitura no mount
trap 'rm -rf "$work"' EXIT

fail=0
count=0
for case_dir in "$TESTS_DIR"/*/; do
  [ -f "${case_dir}sources.txt" ] || continue
  name="$(basename "$case_dir")"
  count=$((count + 1))
  stmt="$work/${name}.sql"
  : > "$stmt"
  while read -r f || [ -n "$f" ]; do
    [ -n "$f" ] || continue
    cat "$KSQLDIR/$f" >> "$stmt"
    printf '\n' >> "$stmt"
  done < "${case_dir}sources.txt"
  sed -i -e '/^SET /d' -e 's/EMIT FINAL/EMIT CHANGES/' "$stmt"

  echo "== caso: $name =="
  out="$(docker run --rm \
    -v "$work":/work \
    -v "${case_dir%/}":/case:ro \
    --entrypoint ksql-test-runner "$IMAGE" \
    -s "/work/${name}.sql" -i /case/input.json -o /case/expected.json 2>&1 || true)"
  if grep -qE '>>> Test passed!' <<<"$out"; then
    echo "  OK"
  else
    echo "  FALHOU"
    grep -E '>>>>|Expected|Invalid arguments|not match' <<<"$out" | sed 's/^/    /' || true
    fail=1
  fi
done

if [ "$count" -eq 0 ]; then
  echo "Nenhum caso de teste em $TESTS_DIR (esperado <caso>/sources.txt)."
  exit 1
fi

echo
if [ "$fail" -ne 0 ]; then
  echo "ksql-test: FALHOU"
  exit 1
fi
echo "ksql-test: OK — $count caso(s)."
