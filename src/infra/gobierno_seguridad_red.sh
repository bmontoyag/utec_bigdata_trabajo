#!/usr/bin/env bash
# Capa transversal de Lumina Bank (laboratorio): redes, seguridad y gobierno de datos.
# Requiere haber ejecutado infra/setup.sh y cargado transacciones_batch (README, paso 4).
# Uso: export PROJECT_ID=mi-proyecto && bash infra/gobierno_seguridad_red.sh   (ejecutar desde src/)
#      CON_NAT=1 bash infra/gobierno_seguridad_red.sh   # agrega Cloud NAT (≈ USD 1/día; solo si los workers
#                                                       # sin IP pública deben instalar paquetes de PyPI)
set -euo pipefail

: "${PROJECT_ID:?Defina PROJECT_ID}"
REGION="${REGION:-us-central1}"
KMS_LOC="us"  # misma ubicación que los buckets y datasets (multirregión US)
SUBRED="lumina-datos-${REGION}"
SA="sa-lumina-pipelines@${PROJECT_ID}.iam.gserviceaccount.com"
PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
TOKEN="$(gcloud auth print-access-token)"

# Los agentes de servicio de un proyecto nuevo tardan en existir: reintenta hasta 6 veces
reintentar() {
  for intento in 1 2 3 4 5 6; do
    "$@" >/dev/null 2>&1 && return 0
    echo "   esperando a que exista la identidad (intento ${intento})"; sleep 20
  done
  "$@"
}

gcloud services enable compute.googleapis.com cloudkms.googleapis.com datacatalog.googleapis.com \
  dataplex.googleapis.com logging.googleapis.com secretmanager.googleapis.com

# ------------------------------------------------------------------ REDES
echo "==> VPC privada y subred con Private Google Access"
gcloud compute networks create lumina-vpc --subnet-mode=custom 2>/dev/null || echo "   lumina-vpc ya existe"
gcloud compute networks subnets create "$SUBRED" --network=lumina-vpc --region="$REGION" \
  --range=10.10.0.0/20 --enable-private-ip-google-access --enable-flow-logs 2>/dev/null \
  || echo "   ${SUBRED} ya existe"
# Tráfico interno entre workers de Dataflow (shuffle) y nodos de Dataproc; nada abierto a Internet
gcloud compute firewall-rules create lumina-interno --network=lumina-vpc --direction=INGRESS \
  --action=ALLOW --rules=tcp,udp,icmp --source-ranges=10.10.0.0/20 2>/dev/null || true
# Acceso SSH solo a través de Identity-Aware Proxy (sin IP pública)
gcloud compute firewall-rules create lumina-iap-ssh --network=lumina-vpc --direction=INGRESS \
  --action=ALLOW --rules=tcp:22 --source-ranges=35.235.240.0/20 2>/dev/null || true
if [[ "${CON_NAT:-0}" == "1" ]]; then
  gcloud compute routers create lumina-router --network=lumina-vpc --region="$REGION" 2>/dev/null || true
  gcloud compute routers nats create lumina-nat --router=lumina-router --region="$REGION" \
    --auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges 2>/dev/null || true
fi

# ------------------------------------------------------------------ SEGURIDAD
echo "==> Cloud KMS: llave CMEK con rotación cada 90 días"
gcloud kms keyrings create lumina-kr --location="$KMS_LOC" 2>/dev/null || echo "   keyring ya existe"
PROXIMA="$(python3 -c "import datetime as d; print((d.datetime.utcnow()+d.timedelta(days=90)).strftime('%Y-%m-%dT%H:%M:%SZ'))")"
gcloud kms keys create lumina-datos --keyring=lumina-kr --location="$KMS_LOC" --purpose=encryption \
  --rotation-period=90d --next-rotation-time="$PROXIMA" 2>/dev/null || echo "   llave ya existe"
LLAVE="projects/${PROJECT_ID}/locations/${KMS_LOC}/keyRings/lumina-kr/cryptoKeys/lumina-datos"

# Los agentes de Cloud Storage y BigQuery cifran con la llave (el usuario nunca la ve)
gcloud storage service-agent --project="$PROJECT_ID" >/dev/null          # crea el agente si no existe
bq show --encryption_service_account --project_id="$PROJECT_ID" >/dev/null # idem
GCS_AGENTE="service-${PROJECT_NUMBER}@gs-project-accounts.iam.gserviceaccount.com"
BQ_AGENTE="bq-${PROJECT_NUMBER}@bigquery-encryption.iam.gserviceaccount.com"
for agente in "$GCS_AGENTE" "$BQ_AGENTE"; do
  reintentar gcloud kms keys add-iam-policy-binding lumina-datos --keyring=lumina-kr --location="$KMS_LOC" \
    --member="serviceAccount:${agente}" --role=roles/cloudkms.cryptoKeyEncrypterDecrypter
done
gcloud storage buckets update "gs://${PROJECT_ID}-lumina-archive" --default-encryption-key="$LLAVE"
bq update --default_kms_key="$LLAVE" "${PROJECT_ID}:lumina_gold"   # tablas nuevas de Gold quedan con CMEK

echo "==> Auditoría: logs de Cloud Audit → BigQuery (lumina_auditoria)"
bq --location=US mk --dataset --description "Auditoría: Cloud Audit Logs" "${PROJECT_ID}:lumina_auditoria" >/dev/null 2>&1 \
  || echo "   lumina_auditoria ya existe"
gcloud logging sinks create lumina-auditoria \
  "bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/lumina_auditoria" \
  --log-filter='logName:"cloudaudit.googleapis.com"' --use-partitioned-tables 2>/dev/null || true
ESCRITOR="$(gcloud logging sinks describe lumina-auditoria --format='value(writerIdentity)')"
reintentar gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="$ESCRITOR" \
  --role=roles/bigquery.dataEditor --condition=None --quiet

# ------------------------------------------------------------------ GOBIERNO
echo "==> Policy tags (Data Catalog): taxonomía de sensibilidad"
TAXONOMIA="$(curl -s -X POST -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" \
  "https://datacatalog.googleapis.com/v1/projects/${PROJECT_ID}/locations/us/taxonomies" \
  -d '{"displayName":"lumina_sensibilidad","description":"Clasificación de datos de Lumina Bank",
       "activatedPolicyTypes":["FINE_GRAINED_ACCESS_CONTROL"]}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('name',''))")"
if [[ -n "$TAXONOMIA" ]]; then
  for tag in "PII_ALTA:montos y saldos por cliente, identificadores" "PII_MEDIA:geolocalización y dispositivo"; do
    curl -s -X POST -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" \
      "https://datacatalog.googleapis.com/v1/${TAXONOMIA}/policyTags" \
      -d "{\"displayName\":\"${tag%%:*}\",\"description\":\"${tag#*:}\"}" >/dev/null
  done
  echo "   taxonomía creada: ${TAXONOMIA}"
  echo "   Asigne los tags en BigQuery → lumina_silver.transacciones_batch → Esquema → Editar"
  echo "   (cliente_id, saldo_* → PII_ALTA; lat, lon → PII_MEDIA)."
else
  echo "   la taxonomía ya existe (o falló la creación): revise Dataplex → Policy tags"
fi

echo "==> Dataplex: escaneo de calidad sobre lumina_silver.transacciones_batch"
cat > /tmp/lumina-dq.yaml <<'EOF'
samplingPercent: 10
rules:
- column: event_id
  dimension: UNIQUENESS
  uniquenessExpectation: {}
- column: cliente_id
  dimension: COMPLETENESS
  nonNullExpectation: {}
- column: monto
  dimension: VALIDITY
  rangeExpectation: {minValue: '0'}
- column: tipo
  dimension: VALIDITY
  setExpectation: {values: [CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER]}
- column: distrito_id
  dimension: VALIDITY
  rangeExpectation: {minValue: '1', maxValue: '8'}
EOF
if gcloud dataplex datascans describe lumina-dq-transacciones --location="$REGION" >/dev/null 2>&1; then
  echo "   el escaneo ya existe"
else
  # la creación tarda 2–3 minutos en un proyecto nuevo
  gcloud dataplex datascans create data-quality lumina-dq-transacciones --location="$REGION" \
    --data-source-resource="//bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/lumina_silver/tables/transacciones_batch" \
    --data-quality-spec-file=/tmp/lumina-dq.yaml >/dev/null
fi
gcloud dataplex datascans run lumina-dq-transacciones --location="$REGION" --format="value(job.name)"

echo "==> Vistas por entidad y señal de fraude compartida"
sed "s/PROJECT_ID/${PROJECT_ID}/g" sql/05_gobierno_seguridad.sql | bq query --use_legacy_sql=false --location=US

echo "==> Listo. Lance Dataflow con: --no_use_public_ips --subnetwork regions/${REGION}/subnetworks/${SUBRED}"
