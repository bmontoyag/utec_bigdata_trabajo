"""Registra en Data Lineage (Dataplex) el recorrido de punta a punta de los datos de Lumina Bank.

Spark (Dataproc) ya publica su linaje solo con --properties spark.dataproc.lineage.enabled=true.
Los pasos de BigQuery (carga a Silver, SQL de Gold) y el streaming se registran aquí con eventos
OpenLineage, para que la pestaña Lineage de cada tabla muestre el camino completo.

Uso (desde src/):  PROJECT_ID=mi-proyecto python3 infra/registrar_linaje.py
"""
import json
import os
import subprocess
import urllib.request
import uuid
from datetime import datetime, timezone

PROYECTO = os.environ["PROJECT_ID"]
UBICACION = "us"  # misma multirregión que los datasets
URL = f"https://datalineage.googleapis.com/v1/projects/{PROYECTO}/locations/{UBICACION}:processOpenLineageRunEvent"
TOKEN = subprocess.run(["gcloud", "auth", "print-access-token"], capture_output=True, text=True, check=True).stdout.strip()


def gcs(bucket, ruta):
    return {"namespace": f"gs://{PROYECTO}-{bucket}", "name": ruta}


def bq(tabla):
    return {"namespace": "bigquery", "name": f"{PROYECTO}.{tabla}"}


def publico(tabla):
    return {"namespace": "bigquery", "name": f"bigquery-public-data.{tabla}"}


# (trabajo, entradas, salidas)
PASOS = [
    ("pubsub_a_dataflow_fraude", [{"namespace": "pubsub", "name": f"topic:{PROYECTO}:lumina-transacciones"}],
     [bq("lumina_bronze.transacciones_raw"), bq("lumina_silver.transacciones_stream")]),
    # Spark ya lo publica en us-central1; se repite en "us" para que el grafo de BigQuery lo encadene
    ("dataproc_etl_paysim", [gcs("lumina-raw", "paysim")], [gcs("lumina-curated", "transacciones")]),
    ("bq_load_silver_batch", [gcs("lumina-curated", "transacciones")], [bq("lumina_silver.transacciones_batch")]),
    ("gold_dim_cliente", [bq("lumina_bronze.paysim_raw")], [bq("lumina_gold.dim_cliente")]),
    ("gold_dim_distrito", [publico("google_trends.international_top_terms")],
     [bq("lumina_gold.dim_distrito")]),
    ("gold_dim_entidad", [publico("sec_quarterly_financials.quick_summary")],
     [bq("lumina_gold.dim_entidad")]),
    ("gold_dim_fecha", [bq("lumina_bronze.feriados")], [bq("lumina_gold.dim_fecha")]),
    ("gold_fact_transacciones", [bq("lumina_silver.transacciones_batch"), bq("lumina_silver.transacciones_stream")],
     [bq("lumina_gold.fact_transacciones")]),
    ("gold_fact_liquidez_hora", [bq("lumina_gold.fact_transacciones"), bq("lumina_gold.dim_distrito"),
                                 bq("lumina_gold.dim_fecha")], [bq("lumina_gold.fact_liquidez_hora")]),
    ("gold_kpi_operativos", [bq("lumina_gold.fact_transacciones"), bq("lumina_gold.dim_tipo_transaccion")],
     [bq("lumina_gold.v_kpi_operativos_diario"), bq("lumina_gold.v_ingreso_por_producto")]),
    ("gold_congestion", [bq("lumina_gold.fact_transacciones"), bq("lumina_gold.dim_distrito")],
     [bq("lumina_gold.v_congestion_distrito_hora")]),
    ("gold_reglas_fraude", [bq("lumina_silver.transacciones_stream")], [bq("lumina_gold.v_desempeno_reglas_fraude")]),
    ("gold_benchmark_sector", [publico("sec_quarterly_financials.quick_summary"),
                               bq("lumina_gold.dim_entidad")], [bq("lumina_gold.fact_benchmark_sector")]),
    ("gold_salud_vs_sector", [bq("lumina_gold.fact_benchmark_sector")], [bq("lumina_gold.v_salud_financiera_vs_sector")]),
    ("gobierno_vista_entidad", [bq("lumina_silver.transacciones_batch"), bq("lumina_gold.acceso_entidad")],
     [bq("lumina_gold.v_transacciones_entidad")]),
]


def enviar(trabajo, entradas, salidas):
    ahora = datetime.now(timezone.utc).isoformat()
    corrida = str(uuid.uuid4())
    for tipo in ("START", "COMPLETE"):
        evento = {"eventType": tipo, "eventTime": ahora, "producer": "https://github.com/lumina-bank/registrar_linaje",
                  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent",
                  "run": {"runId": corrida}, "job": {"namespace": "lumina-bank", "name": trabajo},
                  "inputs": entradas, "outputs": salidas}
        req = urllib.request.Request(URL, data=json.dumps(evento).encode(), method="POST", headers={
            "Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json", "x-goog-user-project": PROYECTO})
        urllib.request.urlopen(req).read()
    print("OK", trabajo, len(entradas), "→", len(salidas))


if __name__ == "__main__":
    for paso in PASOS:
        enviar(*paso)
