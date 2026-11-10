#!/bin/bash
# Uso: metrica.sh <tipo de métrica> <filtro extra> <minutos hacia atrás>
# Consulta Cloud Monitoring (API v3) y muestra una serie por minuto: hora UTC y valor.
TIPO="$1"; EXTRA="$2"; MIN="${3:-30}"
FIN=$(date -u +%Y-%m-%dT%H:%M:%SZ); INI=$(date -u -v-${MIN}M +%Y-%m-%dT%H:%M:%SZ)
curl -s -G "https://monitoring.googleapis.com/v3/projects/lumina-bank-utec/timeSeries" \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  --data-urlencode "filter=metric.type=\"${TYPE:-$TIPO}\" ${EXTRA}" \
  --data-urlencode "interval.startTime=${INI}" --data-urlencode "interval.endTime=${FIN}" \
  --data-urlencode "aggregation.alignmentPeriod=60s" --data-urlencode "aggregation.perSeriesAligner=ALIGN_MAX" |
python3 -c '
import sys, json
d = json.load(sys.stdin)
for s in d.get("timeSeries", []):
    print("#", s["metric"].get("labels", {}), s["resource"].get("labels", {}).get("subscription_id", ""))
    for p in reversed(s["points"]):
        v = p["value"]; v = v.get("doubleValue", v.get("int64Value"))
        print(p["interval"]["endTime"][11:16], v)
'
