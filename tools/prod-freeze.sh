#!/usr/bin/env bash
# Congela / reativa a prod do Pulso para economizar custo.
#
#   bash tools/prod-freeze.sh freeze     # para tudo (VM, SQL, workers, schedulers)
#   bash tools/prod-freeze.sh unfreeze   # religa tudo
#   bash tools/prod-freeze.sh status     # mostra o estado atual
#
# Tudo é REVERSÍVEL: a VM é parada (disco de dados intacto), o Cloud SQL recebe
# activation-policy NEVER (dados preservados, deletion_protection ligado), os
# workers vão a min-instances=0 e os schedulers são pausados. Nada é deletado.
#
# Custo enquanto congelado ~= storage (GCS/disco/SQL parado) + Artifact Registry.
# As partes que cobram por hora de compute (VM e2-standard-2, workers always-on)
# ficam zeradas.
set -euo pipefail

REGION="us-central1"
ZONE="us-central1-a"
VM="pulso-redpanda"
SQL="pulso-catalog-prod"
WORKERS=(pulso-ingest pulso-sink pulso-anomaly-detector pulso-llm-explainer)
SCHEDULERS=(pulso-soda-check-hourly pulso-lake-mirror-hourly pulso-iceberg-maintain-daily)

log() { printf '\n\033[1m▸ %s\033[0m\n' "$*"; }

freeze() {
  log "Pausando schedulers (evita jobs disparando com a VM parada)"
  for j in "${SCHEDULERS[@]}"; do
    gcloud scheduler jobs pause "$j" --location "$REGION" --quiet || true
  done

  log "Workers Cloud Run -> min-instances=0 (deixam de ficar always-on)"
  for s in "${WORKERS[@]}"; do
    gcloud run services update "$s" --region "$REGION" --min-instances=0 --quiet \
      --format="value(metadata.name)"
  done

  log "Parando Cloud SQL ($SQL) — activation-policy NEVER"
  gcloud sql instances patch "$SQL" --activation-policy NEVER --quiet

  log "Parando a VM Redpanda ($VM) — disco de dados preservado"
  gcloud compute instances stop "$VM" --zone "$ZONE" --quiet

  log "FREEZE concluído. pulso-serve segue acessível (escala a zero); o painel ao"
  log "vivo ficará vazio até reativar. Rode 'unfreeze' na semana que vem."
}

unfreeze() {
  log "Iniciando a VM Redpanda ($VM) — o startup script sobe Kafka/ksqlDB (~3-5 min)"
  gcloud compute instances start "$VM" --zone "$ZONE" --quiet

  log "Religando Cloud SQL ($SQL) — activation-policy ALWAYS"
  gcloud sql instances patch "$SQL" --activation-policy ALWAYS --quiet

  log "Workers Cloud Run -> min-instances=1 (worker contínuo)"
  for s in "${WORKERS[@]}"; do
    gcloud run services update "$s" --region "$REGION" --min-instances=1 --quiet \
      --format="value(metadata.name)"
  done

  log "Retomando schedulers"
  for j in "${SCHEDULERS[@]}"; do
    gcloud scheduler jobs resume "$j" --location "$REGION" --quiet || true
  done

  log "UNFREEZE disparado. Aguarde ~3-5 min o Redpanda subir; os workers reconectam"
  log "sozinhos. Verifique: curl .../api/candles/live?symbol=BTC-USD&interval=M1"
}

status() {
  log "VM"
  gcloud compute instances list --filter="name=$VM" \
    --format="table(name,status)" 2>/dev/null || true
  log "Cloud SQL"
  gcloud sql instances list --filter="name=$SQL" \
    --format="table(name,state,settings.activationPolicy)" 2>/dev/null || true
  log "Workers (minScale)"
  for s in "${WORKERS[@]}" pulso-serve pulso-marquez; do
    echo "$s=$(gcloud run services describe "$s" --region "$REGION" \
      --format="value(spec.template.metadata.annotations['autoscaling.knative.dev/minScale'])" 2>/dev/null)"
  done
  log "Schedulers"
  gcloud scheduler jobs list --location "$REGION" \
    --format="table(name.basename(),state)" 2>/dev/null || true
}

case "${1:-}" in
  freeze)   freeze ;;
  unfreeze) unfreeze ;;
  status)   status ;;
  *) echo "uso: $0 {freeze|unfreeze|status}" >&2; exit 2 ;;
esac
