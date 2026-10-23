"""Pipeline streaming de detección de fraude (Apache Beam / Dataflow).

Pub/Sub tx-eventos → parseo/validación → estado por cliente (viaje imposible)
→ estado por destino (cuenta mula) → reglas → BigQuery + Pub/Sub alertas (+ Memorystore opcional)

Ejecución en Dataflow:
  python fraud_streaming_pipeline.py \
    --project $PROJECT_ID --region us-central1 --runner DataflowRunner \
    --temp_location gs://$PROJECT_ID-lumina-temp/dataflow \
    --service_account_email sa-lumina-pipelines@$PROJECT_ID.iam.gserviceaccount.com \
    --enable_streaming_engine --max_num_workers 3 --machine_type n1-standard-2 \
    --job_name lumina-fraude-stream
"""
import argparse
import json
import logging
import math
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec, TimerSpec, on_timer
from apache_beam.utils.timestamp import Duration, Timestamp

CAMPOS_REQUERIDOS = ("event_id", "event_ts", "tipo", "monto", "cliente_id", "destino_id",
                     "saldo_ant_orig", "saldo_nuevo_orig", "saldo_ant_dest", "saldo_nuevo_dest")
CAMPOS_NUMERICOS = ("monto", "saldo_ant_orig", "saldo_nuevo_orig", "saldo_ant_dest", "saldo_nuevo_dest")
COLUMNAS_TX = ("event_id", "event_ts", "ingest_ts", "decision_ts", "step", "tipo", "monto", "cliente_id",
               "saldo_ant_orig", "saldo_nuevo_orig", "destino_id", "saldo_ant_dest", "saldo_nuevo_dest",
               "canal", "entidad_id", "distrito_id", "lat", "lon", "device_id", "is_fraud_label",
               "sintetico", "velocidad_kmh", "tx_destino_10min", "score", "decision", "reglas", "latencia_ms")
COLUMNAS_ALERTA = ("event_id", "event_ts", "decision_ts", "cliente_id", "destino_id", "distrito_id",
                   "entidad_id", "tipo", "monto", "score", "decision", "reglas", "is_fraud_label")

VELOCIDAD_MAX_KMH = 800
VENTANA_DESTINO_S = 600
UMBRAL_BLOQUEAR, UMBRAL_REVISAR = 0.8, 0.5


def ahora_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def epoch(ts_iso: str) -> float:
    return datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).timestamp()


def distancia_km(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(a))


class ParsearEvento(beam.DoFn):
    INVALIDOS = "invalidos"

    def process(self, mensaje: bytes):
        try:
            ev = json.loads(mensaje.decode("utf-8"))
            faltan = [c for c in CAMPOS_REQUERIDOS if ev.get(c) is None]
            if faltan:
                raise ValueError(f"faltan campos: {faltan}")
            for c in CAMPOS_NUMERICOS:
                ev[c] = float(ev[c])
            if ev["monto"] < 0:
                raise ValueError("monto negativo")
            epoch(ev["event_ts"])  # valida formato
            ev["ingest_ts"] = ahora_iso()
            yield ev
        except Exception as e:  # noqa: BLE001 - todo error va a la DLQ
            yield beam.pvalue.TaggedOutput(self.INVALIDOS, json.dumps(
                {"error": str(e), "payload": mensaje.decode("utf-8", "replace")}).encode("utf-8"))


class ViajeImposible(beam.DoFn):
    """Estado por cliente: última ubicación conocida → velocidad implícita en km/h."""
    ULTIMO = ReadModifyWriteStateSpec("ultimo", StrUtf8Coder())
    EXPIRA = TimerSpec("expira", TimeDomain.REAL_TIME)

    def process(self, kv, ultimo=beam.DoFn.StateParam(ULTIMO), expira=beam.DoFn.TimerParam(EXPIRA)):
        _, ev = kv
        ev["velocidad_kmh"] = None
        t = epoch(ev["event_ts"])
        previo = ultimo.read()
        if previo and ev.get("lat") is not None:
            p = json.loads(previo)
            horas = max(abs(t - p["t"]) / 3600, 1 / 3600)
            ev["velocidad_kmh"] = round(distancia_km(p["lat"], p["lon"], ev["lat"], ev["lon"]) / horas, 1)
        if ev.get("lat") is not None:
            ultimo.write(json.dumps({"t": t, "lat": ev["lat"], "lon": ev["lon"]}))
            expira.set(Timestamp.now() + Duration(seconds=3600))
        yield ev

    @on_timer(EXPIRA)
    def limpiar(self, ultimo=beam.DoFn.StateParam(ULTIMO)):
        ultimo.clear()


class VelocidadDestino(beam.DoFn):
    """Estado por destino: transacciones recibidas en los últimos 10 min (patrón cuenta mula)."""
    RECIENTES = ReadModifyWriteStateSpec("recientes", StrUtf8Coder())
    EXPIRA = TimerSpec("expira", TimeDomain.REAL_TIME)

    def process(self, kv, recientes=beam.DoFn.StateParam(RECIENTES), expira=beam.DoFn.TimerParam(EXPIRA)):
        _, ev = kv
        t = epoch(ev["event_ts"])
        lista = [x for x in json.loads(recientes.read() or "[]") if abs(t - x) <= VENTANA_DESTINO_S]
        lista.append(t)
        ev["tx_destino_10min"] = len(lista)
        recientes.write(json.dumps(lista[-100:]))
        expira.set(Timestamp.now() + Duration(seconds=VENTANA_DESTINO_S * 2))
        yield ev

    @on_timer(EXPIRA)
    def limpiar(self, recientes=beam.DoFn.StateParam(RECIENTES)):
        recientes.clear()


def evaluar_reglas(ev: dict) -> dict:
    tipo, monto = ev["tipo"], ev["monto"]
    riesgosa = tipo in ("TRANSFER", "CASH_OUT") and monto > 0
    reglas = []
    if riesgosa and abs(ev["saldo_ant_orig"] - monto) < 0.01 and ev["saldo_nuevo_orig"] == 0:
        reglas.append(("R1_VACIADO_CUENTA", 0.5))
    if riesgosa and ev["saldo_ant_dest"] == 0 and ev["saldo_nuevo_dest"] == 0:
        reglas.append(("R2_INCONSISTENCIA_DESTINO", 0.3))
    if tipo == "TRANSFER" and monto > 200_000:
        reglas.append(("R3_MONTO_ALTO", 0.2))
    if (ev.get("velocidad_kmh") or 0) > VELOCIDAD_MAX_KMH:
        reglas.append(("R4_VIAJE_IMPOSIBLE", 0.5))
    if tipo == "TRANSFER" and ev["destino_id"].startswith("C") and ev.get("tx_destino_10min", 0) >= 3:
        reglas.append(("R5_CUENTA_MULA", 0.3))

    score = min(1.0, sum(p for _, p in reglas))
    ev["reglas"] = [r for r, _ in reglas]
    ev["score"] = round(score, 3)
    ev["decision"] = ("BLOQUEAR" if score >= UMBRAL_BLOQUEAR
                      else "REVISAR" if score >= UMBRAL_REVISAR else "APROBAR")
    ev["decision_ts"] = ahora_iso()
    origen = ev.get("enviado_ts") or ev["ingest_ts"]
    ev["latencia_ms"] = int((epoch(ev["decision_ts"]) - epoch(origen)) * 1000)
    return ev


def fila_bigquery(ev: dict, columnas) -> dict:
    fila = {c: ev.get(c) for c in columnas}
    for c in CAMPOS_NUMERICOS:  # NUMERIC se envía como string para no perder precisión
        if fila.get(c) is not None:
            fila[c] = f"{fila[c]:.2f}"
    return fila


class ActualizarFeaturesRedis(beam.DoFn):
    """Opcional: publica features del cliente en Memorystore para la autorización síncrona."""

    def __init__(self, host: str, puerto: int):
        self.host, self.puerto, self.cliente = host, puerto, None

    def setup(self):
        import redis
        self.cliente = redis.Redis(host=self.host, port=self.puerto, socket_timeout=1)

    def process(self, ev):
        clave = f"cliente:{ev['cliente_id']}"
        pipe = self.cliente.pipeline()
        pipe.hset(clave, mapping={"ultimo_score": ev["score"], "ultima_decision": ev["decision"],
                                  "ultimo_lat": ev.get("lat") or "", "ultimo_lon": ev.get("lon") or "",
                                  "ultimo_ts": ev["event_ts"]})
        pipe.hincrby(clave, "tx_24h", 1)
        pipe.expire(clave, 86400)
        pipe.execute()
        yield ev


def run(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--subscription", default="tx-eventos-dataflow")
    ap.add_argument("--dataset", default="lumina_silver")
    ap.add_argument("--topic_alertas", default="alertas-fraude")
    ap.add_argument("--topic_dlq", default="tx-eventos-dlq")
    ap.add_argument("--redis_host", default=None, help="IP de Memorystore (opcional)")
    ap.add_argument("--redis_port", type=int, default=6379)
    args, beam_args = ap.parse_known_args(argv)

    opciones = PipelineOptions(beam_args, project=args.project)
    opciones.view_as(StandardOptions).streaming = True
    opciones.view_as(SetupOptions).save_main_session = True

    p = args.project
    with beam.Pipeline(options=opciones) as pipeline:
        parseados = (
            pipeline
            | "LeerPubSub" >> beam.io.ReadFromPubSub(
                subscription=f"projects/{p}/subscriptions/{args.subscription}", id_label="event_id")
            | "Parsear" >> beam.ParDo(ParsearEvento()).with_outputs(ParsearEvento.INVALIDOS, main="validos"))

        _ = parseados.invalidos | "DLQ" >> beam.io.WriteToPubSub(f"projects/{p}/topics/{args.topic_dlq}")

        evaluados = (
            parseados.validos
            | "KeyCliente" >> beam.Map(lambda e: (e["cliente_id"], e))
            | "ViajeImposible" >> beam.ParDo(ViajeImposible())
            | "KeyDestino" >> beam.Map(lambda e: (e["destino_id"], e))
            | "VelocidadDestino" >> beam.ParDo(VelocidadDestino())
            | "Reglas" >> beam.Map(evaluar_reglas))

        if args.redis_host:
            evaluados = evaluados | "FeaturesRedis" >> beam.ParDo(
                ActualizarFeaturesRedis(args.redis_host, args.redis_port))

        _ = (evaluados
             | "FilaTx" >> beam.Map(fila_bigquery, COLUMNAS_TX)
             | "BQTransacciones" >> beam.io.WriteToBigQuery(
                 f"{p}:{args.dataset}.transacciones_stream",
                 method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
                 create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))

        alertas = evaluados | "SoloAlertas" >> beam.Filter(lambda e: e["decision"] != "APROBAR")
        _ = (alertas
             | "FilaAlerta" >> beam.Map(fila_bigquery, COLUMNAS_ALERTA)
             | "BQAlertas" >> beam.io.WriteToBigQuery(
                 f"{p}:{args.dataset}.alertas_fraude",
                 method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
                 create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
        _ = (alertas
             | "JsonAlerta" >> beam.Map(lambda e: json.dumps(fila_bigquery(e, COLUMNAS_ALERTA)).encode("utf-8"))
             | "PubSubAlertas" >> beam.io.WriteToPubSub(f"projects/{p}/topics/{args.topic_alertas}"))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
