"""API de exposición de datos de Lumina Bank (Cloud Run + FastAPI + BigQuery)."""
import os

from fastapi import FastAPI, HTTPException, Query
from google.cloud import bigquery

PROJECT_ID = os.environ["PROJECT_ID"]
bq = bigquery.Client(project=PROJECT_ID)
app = FastAPI(title="Lumina Bank Data API", version="1.0")


def consultar(sql: str, **params) -> list[dict]:
    tipos = {int: "INT64", str: "STRING", float: "FLOAT64"}
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter(k, tipos[type(v)], v) for k, v in params.items()])
    filas = bq.query(sql.replace("PROJECT_ID", PROJECT_ID), job_config=config).result()
    return [dict(f) for f in filas]


@app.get("/salud")
def salud():
    return {"estado": "ok"}


@app.get("/alertas")
def alertas(minutos: int = Query(60, le=1440), limite: int = Query(100, le=1000)):
    return consultar("""
        SELECT event_id, event_ts, cliente_id, destino_id, distrito_id, tipo, monto, score, decision, reglas
        FROM `PROJECT_ID.lumina_silver.alertas_fraude`
        WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @minutos MINUTE)
        ORDER BY event_ts DESC LIMIT @limite""", minutos=minutos, limite=limite)


@app.get("/clientes/{cliente_id}/riesgo")
def riesgo_cliente(cliente_id: str):
    perfil = consultar("""
        SELECT c.*, d.nombre AS distrito
        FROM `PROJECT_ID.lumina_gold.dim_cliente` c
        JOIN `PROJECT_ID.lumina_gold.dim_distrito` d USING (distrito_id)
        WHERE cliente_id = @cliente_id""", cliente_id=cliente_id)
    if not perfil:
        raise HTTPException(404, "cliente no encontrado")
    recientes = consultar("""
        SELECT event_ts, tipo, monto, canal, score, decision, reglas
        FROM `PROJECT_ID.lumina_silver.transacciones_stream`
        WHERE cliente_id = @cliente_id AND event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        ORDER BY event_ts DESC LIMIT 20""", cliente_id=cliente_id)
    return {"perfil": perfil[0], "transacciones_recientes": recientes,
            "score_maximo_30d": max((t["score"] or 0 for t in recientes), default=0)}


@app.get("/liquidez/{distrito_id}/pronostico")
def pronostico_liquidez(distrito_id: int):
    return consultar("""
        SELECT * FROM `PROJECT_ID.lumina_gold.v_plan_liquidez_t1`
        WHERE distrito_id = @distrito_id ORDER BY fecha""", distrito_id=distrito_id)


@app.get("/kpis")
def kpis(dias: int = Query(7, le=90)):
    return consultar("""
        SELECT fecha, origen, SUM(n_tx) AS n_tx, SUM(monto_total) AS monto_total,
               SUM(ingreso_estimado) AS ingreso_estimado, AVG(tiempo_aprobacion_p95_ms) AS p95_ms
        FROM `PROJECT_ID.lumina_gold.v_kpi_operativos_diario`
        WHERE fecha >= DATE_SUB(CURRENT_DATE(), INTERVAL @dias DAY) OR origen = 'batch'
        GROUP BY 1, 2 ORDER BY fecha DESC LIMIT 100""", dias=dias)
