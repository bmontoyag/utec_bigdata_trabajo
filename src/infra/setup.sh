#!/usr/bin/env bash
# Aprovisiona la infraestructura base de Lumina Bank (laboratorio).
# Uso: export PROJECT_ID=mi-proyecto && bash infra/setup.sh   (ejecutar desde src/)
set -euo pipefail

: "${PROJECT_ID:?Defina PROJECT_ID, p. ej. export PROJECT_ID=lumina-bank-utec}"
REGION="${REGION:-us-central1}"
SA_NAME="sa-lumina-pipelines"
SA="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud config set project "$PROJECT_ID"
PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
PUBSUB_SA="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

echo "==> Habilitando APIs"
gcloud services enable \
  pubsub.googleapis.com dataflow.googleapis.com dataproc.googleapis.com \
  bigquery.googleapis.com bigquerydatatransfer.googleapis.com storage.googleapis.com \
  run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com \
  compute.googleapis.com cloudkms.googleapis.com dlp.googleapis.com dataplex.googleapis.com \
  analyticshub.googleapis.com

echo "==> Buckets del Data Lake (multirregión US, acceso uniforme)"
for zona in raw curated archive temp; do
  gcloud storage buckets create "gs://${PROJECT_ID}-lumina-${zona}" \
    --location=US --uniform-bucket-level-access 2>/dev/null || echo "   bucket ${zona} ya existe"
done
gcloud storage buckets update "gs://${PROJECT_ID}-lumina-raw" --versioning

cat > /tmp/lumina-lifecycle.json <<'EOF'
{"rule": [
  {"action": {"type": "SetStorageClass", "storageClass": "NEARLINE"}, "condition": {"age": 30}},
  {"action": {"type": "SetStorageClass", "storageClass": "COLDLINE"}, "condition": {"age": 90}},
  {"action": {"type": "SetStorageClass", "storageClass": "ARCHIVE"},  "condition": {"age": 365}}
]}
EOF
gcloud storage buckets update "gs://${PROJECT_ID}-lumina-archive" --lifecycle-file=/tmp/lumina-lifecycle.json

echo "==> Cuenta de servicio de pipelines (mínimo privilegio)"
gcloud iam service-accounts create "$SA_NAME" --display-name="Lumina pipelines" 2>/dev/null || true
for rol in roles/dataflow.worker roles/dataproc.worker roles/bigquery.dataEditor roles/bigquery.jobUser \
           roles/pubsub.subscriber roles/pubsub.publisher roles/storage.objectAdmin; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${SA}" --role="$rol" \
    --condition=None --quiet >/dev/null
done

echo "==> Permisos del agente de Pub/Sub (suscripciones a BigQuery y Cloud Storage)"
gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${PUBSUB_SA}" \
  --role=roles/bigquery.dataEditor --condition=None --quiet >/dev/null
gcloud storage buckets add-iam-policy-binding "gs://${PROJECT_ID}-lumina-raw" \
  --member="serviceAccount:${PUBSUB_SA}" --role=roles/storage.objectCreator >/dev/null
gcloud storage buckets add-iam-policy-binding "gs://${PROJECT_ID}-lumina-raw" \
  --member="serviceAccount:${PUBSUB_SA}" --role=roles/storage.legacyBucketReader >/dev/null

echo "==> BigQuery: datasets, Bronze y Silver"
sed "s/PROJECT_ID/${PROJECT_ID}/g" sql/01_ddl.sql | bq query --use_legacy_sql=false --location=US

echo "==> Pub/Sub: tópicos y suscripciones"
for t in tx-eventos tx-eventos-dlq telemetria alertas-fraude; do
  gcloud pubsub topics create "$t" --message-retention-duration=7d 2>/dev/null || echo "   tópico ${t} ya existe"
done
# Consumidor principal: Dataflow
gcloud pubsub subscriptions create tx-eventos-dataflow --topic=tx-eventos \
  --ack-deadline=60 --message-retention-duration=7d 2>/dev/null || true
# Archivo crudo en el Data Lake (auditoría / replay)
gcloud pubsub subscriptions create tx-eventos-archivo --topic=tx-eventos \
  --cloud-storage-bucket="${PROJECT_ID}-lumina-raw" \
  --cloud-storage-file-prefix="stream-archive/tx-eventos/" \
  --cloud-storage-max-duration=5m 2>/dev/null || true
# Telemetría directo a BigQuery (sin código)
gcloud pubsub subscriptions create telemetria-bq --topic=telemetria \
  --bigquery-table="${PROJECT_ID}:lumina_bronze.telemetria_raw" \
  --use-table-schema --write-metadata --drop-unknown-fields 2>/dev/null || true
# Consumidores de alertas y DLQ (para inspección en la demo)
gcloud pubsub subscriptions create alertas-fraude-demo --topic=alertas-fraude 2>/dev/null || true
gcloud pubsub subscriptions create tx-eventos-dlq-revision --topic=tx-eventos-dlq 2>/dev/null || true

echo "==> Listo. Siguiente paso: subir PaySim a gs://${PROJECT_ID}-lumina-raw/paysim/ (ver README)."
