"""Telemetría sintética de cajeros/POS hacia Pub/Sub (topic telemetria → suscripción BigQuery).

Uso:
  python publish_telemetry.py --project $PROJECT_ID --terminales 50 --intervalo 5 --minutos 30
"""
import argparse
import json
import random
import time
from datetime import datetime, timezone

from google.cloud import pubsub_v1

import common


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--topic", default="telemetria")
    ap.add_argument("--terminales", type=int, default=50)
    ap.add_argument("--intervalo", type=float, default=5, help="segundos entre lecturas")
    ap.add_argument("--minutos", type=float, default=30)
    ap.add_argument("--pct-degradadas", type=float, default=0.1,
                    help="fracción de terminales que se degradan (para detectar anomalías)")
    args = ap.parse_args()

    publisher = pubsub_v1.PublisherClient()
    topic = publisher.topic_path(args.project, args.topic)
    terminales = [{
        "terminal_id": f"T-{i:04d}",
        "distrito_id": common.bucket(f"T-{i:04d}", "d", common.N_DISTRITOS) + 1,
        "tipo_terminal": random.choice(["ATM", "ATM", "POS", "CORRESPONSAL"]),
        "efectivo_pct": random.uniform(60, 100),
        "degradada": random.random() < args.pct_degradadas,
    } for i in range(args.terminales)]

    fin = time.time() + args.minutos * 60
    ciclo = 0
    while time.time() < fin:
        for t in terminales:
            deriva = ciclo * 8 if t["degradada"] else 0
            latencia = max(20, int(random.gauss(120, 25) + deriva))
            t["efectivo_pct"] = max(0.0, t["efectivo_pct"] - random.uniform(0, 0.4))
            estado = "ERROR" if latencia > 1500 else ("DEGRADADO" if latencia > 600 else "OK")
            ev = {
                "terminal_id": t["terminal_id"],
                "distrito_id": t["distrito_id"],
                "tipo_terminal": t["tipo_terminal"],
                "event_ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "cpu_pct": round(min(100, random.gauss(40, 10) + deriva / 20), 1),
                "temp_c": round(random.gauss(38, 2) + deriva / 50, 1),
                "latencia_ms": latencia,
                "efectivo_pct": round(t["efectivo_pct"], 1),
                "estado": estado,
            }
            publisher.publish(topic, json.dumps(ev).encode("utf-8"))
        ciclo += 1
        print(f"ciclo {ciclo}: {len(terminales)} lecturas")
        time.sleep(args.intervalo)


if __name__ == "__main__":
    main()
