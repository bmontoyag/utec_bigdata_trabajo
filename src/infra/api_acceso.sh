#!/usr/bin/env bash
# Cuenta de servicio propia para la API y vistas autorizadas (Entrega 3).
# La API solo puede leer lumina_api; las vistas de ese dataset leen Silver, Gold y ML en su nombre.
# Uso (desde src/): bash infra/api_acceso.sh   (antes: sql/07_api.sql)
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-lumina-bank-utec}"
SA_API="sa-lumina-api@${PROJECT_ID}.iam.gserviceaccount.com"

echo "==> Cuenta de servicio de la API"
gcloud iam service-accounts describe "$SA_API" >/dev/null 2>&1 || \
  gcloud iam service-accounts create sa-lumina-api --display-name="Lumina API de datos (solo lectura de lumina_api)"
# Ejecutar consultas en el proyecto; no da acceso a ningún dato por sí solo.
# Una cuenta recién creada tarda unos segundos en ser visible para IAM: se reintenta.
for intento in 1 2 3 4 5 6; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${SA_API}" \
    --role=roles/bigquery.jobUser --condition=None >/dev/null 2>&1 && break
  echo "   esperando a que IAM vea la cuenta (intento ${intento})"; sleep 10
done

# Agrega una entrada al access de un dataset si no está (bq no tiene un comando para esto).
agregar_acceso() {
  local dataset="$1" entrada="$2" tmp
  tmp="$(mktemp)"
  bq show --format=prettyjson "${PROJECT_ID}:${dataset}" > "$tmp"
  python3 - "$tmp" "$entrada" <<'EOF'
import json, sys
ruta, entrada = sys.argv[1], json.loads(sys.argv[2])
d = json.load(open(ruta))
if entrada not in d["access"]:
    d["access"].append(entrada)
json.dump({"access": d["access"]}, open(ruta, "w"))
EOF
  bq update --source "$tmp" "${PROJECT_ID}:${dataset}" >/dev/null
  rm -f "$tmp"
}

echo "==> Lectura de lumina_api para la API"
agregar_acceso lumina_api "{\"role\": \"READER\", \"userByEmail\": \"${SA_API}\"}"

echo "==> Datasets autorizados: las vistas de lumina_api leen Silver, Gold y ML"
for origen in lumina_silver lumina_gold lumina_ml; do
  agregar_acceso "$origen" "{\"dataset\": {\"dataset\": {\"projectId\": \"${PROJECT_ID}\", \"datasetId\": \"lumina_api\"}, \"targetTypes\": [\"VIEWS\"]}}"
done
# Las vistas de Gold que lee la API (plan de liquidez, KPI) a su vez leen Silver y ML.
for origen in lumina_silver lumina_ml; do
  agregar_acceso "$origen" "{\"dataset\": {\"dataset\": {\"projectId\": \"${PROJECT_ID}\", \"datasetId\": \"lumina_gold\"}, \"targetTypes\": [\"VIEWS\"]}}"
done
echo "Listo. Despliegue la API con --service-account ${SA_API}"
