"""Reproduce PaySim como flujo en vivo hacia Pub/Sub (topic tx-eventos).

Uso:
  python publish_transactions.py --project $PROJECT_ID --csv paysim.csv --rate 50 --limit 20000
  python publish_transactions.py --project $PROJECT_ID --csv paysim.csv --solo-transfer-cashout --rate 200
"""
import argparse
import csv
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from google.cloud import pubsub_v1

import common


def iso(ts: datetime) -> str:
    return ts.isoformat(timespec="microseconds")


def construir_evento(fila: dict, ahora: datetime) -> dict:
    cliente = fila["nameOrig"]
    lat, lon = common.domicilio(cliente)
    return {
        "event_id": str(uuid.uuid4()),
        "event_ts": iso(ahora),
        "enviado_ts": iso(ahora),
        "step": int(fila["step"]),
        "tipo": fila["type"],
        "monto": round(float(fila["amount"]), 2),
        "cliente_id": cliente,
        "saldo_ant_orig": round(float(fila["oldbalanceOrg"]), 2),
        "saldo_nuevo_orig": round(float(fila["newbalanceOrig"]), 2),
        "destino_id": fila["nameDest"],
        "saldo_ant_dest": round(float(fila["oldbalanceDest"]), 2),
        "saldo_nuevo_dest": round(float(fila["newbalanceDest"]), 2),
        "canal": common.canal(cliente, fila["type"]),
        "entidad_id": common.entidad_id(cliente),
        "distrito_id": common.distrito_id(cliente),
        "lat": round(lat + random.uniform(-0.005, 0.005), 6),
        "lon": round(lon + random.uniform(-0.005, 0.005), 6),
        "device_id": f"D-{common.bucket(cliente, 'dev', 10**8)}",
        "is_fraud_label": int(fila["isFraud"]),
        "sintetico": False,
    }


def eventos_viaje_imposible(ev: dict, ahora: datetime) -> list[dict]:
    """Simulación: el cliente pagó en casa hace 2 min y ahora opera a ~600 km."""
    previo = dict(ev, event_id=str(uuid.uuid4()), tipo="PAYMENT", monto=25.0,
                  destino_id="M" + str(random.randint(10**8, 10**9)),
                  event_ts=iso(ahora - timedelta(minutes=2)), canal="APP",
                  is_fraud_label=0, sintetico=True)
    lejano = dict(ev, lat=round(ev["lat"] - 4.0, 6), lon=round(ev["lon"] + 2.5, 6),
                  device_id="D-" + uuid.uuid4().hex[:8])
    return [previo, lejano]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--topic", default="tx-eventos")
    ap.add_argument("--csv", required=True, help="Ruta local al CSV de PaySim")
    ap.add_argument("--rate", type=float, default=50, help="eventos por segundo")
    ap.add_argument("--limit", type=int, default=10_000)
    ap.add_argument("--solo-transfer-cashout", action="store_true",
                    help="publica solo TRANSFER/CASH_OUT (donde ocurre el fraude)")
    ap.add_argument("--prob-viaje-imposible", type=float, default=0.5,
                    help="probabilidad de inyectar geolocalización lejana en filas fraudulentas")
    args = ap.parse_args()

    publisher = pubsub_v1.PublisherClient(
        batch_settings=pubsub_v1.types.BatchSettings(max_messages=500, max_latency=0.05))
    topic = publisher.topic_path(args.project, args.topic)
    intervalo = 1.0 / args.rate
    enviados, futuros = 0, []
    inicio = time.time()

    with open(args.csv, newline="") as f:
        for fila in csv.DictReader(f):
            if args.solo_transfer_cashout and fila["type"] not in ("TRANSFER", "CASH_OUT"):
                continue
            ahora = datetime.now(timezone.utc)
            ev = construir_evento(fila, ahora)
            lote = [ev]
            if ev["is_fraud_label"] == 1 and random.random() < args.prob_viaje_imposible:
                lote = eventos_viaje_imposible(ev, ahora)
            for e in lote:
                futuros.append(publisher.publish(
                    topic, json.dumps(e).encode("utf-8"), event_id=e["event_id"]))
                enviados += 1
            if enviados >= args.limit:
                break
            # control de tasa
            espera = inicio + enviados * intervalo - time.time()
            if espera > 0:
                time.sleep(espera)
            if enviados % 1000 == 0:
                print(f"{enviados} eventos publicados ({enviados / (time.time() - inicio):.0f} ev/s)")

    for fut in futuros:
        fut.result()
    print(f"Listo: {enviados} eventos en {time.time() - inicio:.1f}s")


if __name__ == "__main__":
    main()
