#!/usr/bin/env bash
# Demo end-to-end de la Entrega 3: simulador → Pub/Sub → Dataflow → BigQuery → API (Cloud Run) → tablero.
#
# Uso (desde src/):   bash demo/demo.sh <paso>
#   plantilla   guarda el pipeline como plantilla de Dataflow (una sola vez; necesita apache-beam)
#   datos       baja una muestra de PaySim desde la zona raw del Data Lake (una sola vez)
#   iniciar     limpia el backlog y lanza el job desde la plantilla (tarda 3–5 min en tener worker)
#   estado      estado del job y backlog de la suscripción
#   publicar    publica transacciones a 20 ev/s (por debajo de la capacidad medida de un worker)
#   verificar   decisiones, latencia, alertas recientes y la misma consulta vía API
#   apagar      drena el job (deja de cobrar)
set -euo pipefail

export CLOUDSDK_ACTIVE_CONFIG_NAME="${CLOUDSDK_ACTIVE_CONFIG_NAME:-lumina}"
PROJECT_ID="${PROJECT_ID:-lumina-bank-utec}"
REGION="${REGION:-us-central1}"
PYTHON="${PYTHON:-python3}"
TEMP="gs://${PROJECT_ID}-lumina-temp"
PLANTILLA="${TEMP}/templates/lumina-fraude-stream"
SA="sa-lumina-pipelines@${PROJECT_ID}.iam.gserviceaccount.com"
SUBRED="regions/${REGION}/subnetworks/lumina-datos-${REGION}"
CSV="demo/paysim_demo.csv"
RATE="${RATE:-20}"
LIMIT="${LIMIT:-6000}"
MINUTOS="${MINUTOS:-15}"

job_activo() {
  gcloud dataflow jobs list --region "$REGION" --status=active --format="value(id,name)" | awk '$2 ~ /^lumina-demo/ {print $1; exit}'
}

bq_q() { bq query --use_legacy_sql=false --format=pretty "$1"; }

case "${1:-}" in
plantilla)
  # Sin --template_location el mismo comando lanzaría el job directamente.
  (cd dataflow && "$PYTHON" fraud_streaming_pipeline.py --project "$PROJECT_ID" --region "$REGION" \
    --runner DataflowRunner --temp_location "${TEMP}/dataflow" --staging_location "${TEMP}/staging" \
    --template_location "$PLANTILLA")
  ;;
datos)
  # Primeras filas de PaySim: el simulador filtra TRANSFER/CASH_OUT.
  # Lectura por rango de bytes (~20 MB); se descarta la última línea, que queda cortada.
  gcloud storage cat -r 0-20000000 "gs://${PROJECT_ID}-lumina-raw/paysim/paysim.csv" | sed '$d' > "$CSV"
  wc -l "$CSV"
  ;;
iniciar)
  if [[ -n "$(job_activo)" ]]; then echo "Ya hay un job de demo activo: $(job_activo)"; exit 0; fi
  # Descarta mensajes viejos para que la demo empiece sin backlog.
  gcloud pubsub subscriptions seek tx-eventos-dataflow --time="$(date -u +%Y-%m-%dT%H:%M:%SZ)" >/dev/null
  gcloud dataflow jobs run "lumina-demo-$(date +%H%M)" --gcs-location "$PLANTILLA" --region "$REGION" \
    --staging-location "${TEMP}/staging" --service-account-email "$SA" \
    --worker-machine-type n2-standard-2 --num-workers 1 --max-workers 1 --enable-streaming-engine \
    --disable-public-ips --network lumina-vpc --subnetwork "$SUBRED" \
    --format="yaml(id,name,currentState)"
  echo "Publique recién cuando 'estado' diga que el worker está listo (3–5 min)."
  ;;
estado)
  gcloud dataflow jobs list --region "$REGION" --status=active --format="table(id,name,state,creationTime)"
  echo
  JOB="$(job_activo)"
  if [[ -n "$JOB" ]]; then
    echo "Último aviso del job:"
    curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" \
      "https://dataflow.googleapis.com/v1b3/projects/${PROJECT_ID}/locations/${REGION}/jobs/${JOB}/messages?minimumImportance=JOB_MESSAGE_BASIC&pageSize=100" |
      "$PYTHON" -c '
import json, sys
m = [x for x in json.load(sys.stdin).get("jobMessages", []) if x.get("messageImportance") != "JOB_MESSAGE_DETAILED"]
listo = any("began to receive work" in x["messageText"] for x in m)
print("  " + (m[-1]["messageText"][:160] if m else "(sin avisos todavía)"))
print("  WORKER LISTO: puede publicar" if listo else "  Worker todavía arrancando")'
  fi
  echo
  echo "Mensajes sin confirmar en tx-eventos-dataflow (último minuto):"
  bash ../doc/evidencias/metrica.sh pubsub.googleapis.com/subscription/num_undelivered_messages \
    'resource.labels.subscription_id="tx-eventos-dataflow"' 5 2>/dev/null | tail -3 || true
  ;;
publicar)
  [[ -f "$CSV" ]] || { echo "Falta $CSV: ejecute 'bash demo/demo.sh datos'"; exit 1; }
  (cd simulator && "$PYTHON" publish_transactions.py --project "$PROJECT_ID" --csv "../$CSV" \
    --solo-transfer-cashout --rate "$RATE" --limit "$LIMIT")
  ;;
verificar)
  echo "== Decisiones y latencia de los últimos ${MINUTOS} min (Silver)"
  bq_q "SELECT decision, COUNT(*) AS n, APPROX_QUANTILES(latencia_ms, 100)[OFFSET(95)] AS p95_ms
        FROM lumina_silver.transacciones_stream
        WHERE ingest_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${MINUTOS} MINUTE)
        GROUP BY 1 ORDER BY 2 DESC"
  echo "== Alertas más recientes"
  bq_q "SELECT FORMAT_TIMESTAMP('%H:%M:%S', event_ts, 'America/Lima') AS hora, tipo, ROUND(monto) AS monto,
               distrito_id AS dist, score, decision, ARRAY_TO_STRING(reglas, ',') AS reglas
        FROM lumina_silver.alertas_fraude
        WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${MINUTOS} MINUTE)
        ORDER BY event_ts DESC LIMIT 5"
  echo "== La misma información vía API (Cloud Run, con token)"
  URL="$(gcloud run services describe lumina-api --region "$REGION" --format='value(status.url)')"
  curl -s -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
    "${URL}/alertas?minutos=${MINUTOS}&limite=2" | "$PYTHON" -m json.tool
  ;;
apagar)
  for id in $(gcloud dataflow jobs list --region "$REGION" --status=active --format="value(id)"); do
    gcloud dataflow jobs drain "$id" --region "$REGION"
  done
  echo "Drenando. Verifique con 'estado' que no quede ningún job activo."
  ;;
*)
  sed -n '2,12p' "$0"; exit 1
  ;;
esac
