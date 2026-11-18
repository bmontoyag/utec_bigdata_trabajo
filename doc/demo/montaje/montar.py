"""Arma el video de la demo final a partir de las capturas reales tomadas el 18-nov-2026."""
import subprocess
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

V = Path(__file__).parent
RAW, OUT = V / "raw", V / "frames"
OUT.mkdir(exist_ok=True)
W, H, TOP, BOT = 1920, 1080, 120, 96
BG, ACC, TXT, SUB = (15, 17, 21), (66, 133, 244), (240, 242, 245), (170, 176, 186)
F = "/System/Library/Fonts/Helvetica.ttc"
f_esc, f_tit, f_cap, f_mark = (ImageFont.truetype(F, s) for s in (26, 46, 30, 24))

BQ = (330, 120, 1512, 758)
DF = (232, 110, 1500, 784)
# (archivo, escena, título, recorte o None, leyenda inferior, segundos)
PLAN = [
    ("s1_c4.png", 1, "Arquitectura del camino streaming", None,
     "Simulador → Pub/Sub → Dataflow → BigQuery → API y tablero", 24),
    ("s2_gcs.jpg", 2, "El histórico en el Data Lake", (240, 90, 1568, 500),
     "PaySim (492 MB) en la zona raw de Cloud Storage", 12),
    ("s2_dataproc.jpg", 2, "Spark en Dataproc Serverless", (240, 90, 1568, 758),
     "Job batch terminado: 3 min 37 s, 0,45 DCU-hora", 10),
    ("s2_dataproc_red.jpg", 2, "Spark dentro de la VPC privada", (232, 110, 1500, 720),
     "Subred lumina-datos-us-central1 · cuenta de servicio de pipelines", 9),
    ("s2_bq.jpg", 2, "El modelo estrella en BigQuery", BQ,
     "6,36 millones de transacciones batch (julio) + lo que llega por streaming", 14),
    ("s3_graph.jpg", 3, "El pipeline de fraude en Dataflow", DF,
     "Lee Pub/Sub, parsea, DLQ y etapas con estado · 1 worker sin IP pública", 15),
    ("s3_graph_eps.jpg", 3, "Dataflow procesando", DF,
     "Cada etapa procesa unos 23 elementos por segundo", 15),
    ("s4_term.png", 4, "Arranca el simulador", None,
     "Transacciones reales de PaySim publicadas a 20 por segundo", 18),
    ("s4_pubsub.jpg", 4, "Pub/Sub recibe y entrega", DF,
     "Publicados 19,8/s · confirmados por Dataflow 20,1/s: no se acumula backlog", 27),
    ("s5_dec1.jpg", 5, "Decisiones en BigQuery, en vivo", BQ,
     "Aprobar, revisar o bloquear · p95 de 0,8 s", 22),
    ("s5_dec2.jpg", 5, "Tres minutos después", BQ,
     "La misma consulta: de 1 964 a 5 491 transacciones aprobadas", 23),
    ("s5_lat.jpg", 5, "Latencia por minuto", BQ,
     "p95 entre 0,55 y 0,6 s con carga estable · meta < 5 s", 30),
    ("s6_alertas.jpg", 6, "Alertas y reglas que las dispararon", BQ,
     "Vaciado de cuenta, destino inconsistente, monto alto, viaje imposible, cuenta mula", 45),
    ("s7_term.png", 7, "La API en Cloud Run", None,
     "Sin token: 403 · con token: alertas con cliente y destino seudonimizados", 35),
    ("s7_run.jpg", 7, "Métricas de la API", (232, 85, 1500, 784),
     "Solicitudes 2xx y 4xx, latencia e instancias que vuelven a cero", 25),
    ("s8_tablero.jpg", 8, "Tablero en Data Studio", (180, 60, 1330, 784),
     "KPI por origen, transacciones por entidad, alertas y plan de liquidez", 35),
    ("s8_liquidez.jpg", 8, "Plan de efectivo T+1 por distrito", BQ,
     "Pronóstico de BigQuery ML (ARIMA_PLUS)", 25),
    ("s9_term.png", 9, "Apagado del job", None,
     "Drain: termina lo que tiene en vuelo y se apaga", 12),
    ("s9_drain.jpg", 9, "Job drenando", (232, 85, 1250, 600),
     "Solo se paga el tiempo que corrió · en producción escala de 4 a 55 workers", 18),
]


def tarjeta(nombre, titulo, sub):
    im = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(im)
    d.rectangle((0, H // 2 - 4, 140, H // 2 + 4), fill=ACC)
    d.text((180, H // 2 - 90), titulo, font=ImageFont.truetype(F, 72), fill=TXT)
    d.text((182, H // 2 + 20), sub, font=ImageFont.truetype(F, 34), fill=SUB)
    im.save(OUT / nombre)


def cuadro(i, archivo, escena, titulo, recorte, leyenda):
    im = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(im)
    d.text((48, 26), f"ESCENA {escena}", font=f_esc, fill=ACC)
    d.text((48, 58), titulo, font=f_tit, fill=TXT)
    marca = "Lumina Bank · Demo end-to-end en GCP"
    d.text((W - 48 - d.textlength(marca, font=f_mark), 44), marca, font=f_mark, fill=SUB)
    src = Image.open(RAW / archivo).convert("RGB")
    if recorte:
        src = src.crop(recorte)
    caja_w, caja_h = W - 96, H - TOP - BOT - 16
    esc = min(caja_w / src.width, caja_h / src.height)
    src = src.resize((int(src.width * esc), int(src.height * esc)), Image.LANCZOS)
    im.paste(src, ((W - src.width) // 2, TOP + (caja_h - src.height) // 2 + 8))
    d.rectangle((0, H - BOT, W, H), fill=(24, 27, 33))
    d.text((48, H - BOT + 30), leyenda, font=f_cap, fill=TXT)
    nombre = f"{i:02d}.png"
    im.save(OUT / nombre)
    return nombre


secuencia = [("00.png", 6)]
tarjeta("00.png", "Lumina Bank: demo end-to-end", "Fraude en tiempo real sobre Google Cloud · grabado en vivo el 18 de noviembre de 2026")
for i, (archivo, escena, titulo, recorte, leyenda, seg) in enumerate(PLAN, 1):
    secuencia.append((cuadro(i, archivo, escena, titulo, recorte, leyenda), seg))
tarjeta("99.png", "Fin de la demo", "Repositorio: github.com/bmontoyag/utec_bigdata_trabajo · guion en doc/demo/")
secuencia.append(("99.png", 8))

# Un clip por cuadro con un acercamiento lento y fundido; luego se concatenan.
clips = []
for nombre, seg in secuencia:
    clip = OUT / (nombre.replace(".png", ".mp4"))
    fps, n = 30, seg * 30
    vf = (f"scale={W*2}:{H*2},zoompan=z='min(1+0.03*on/{n},1.03)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'"
          f":d={n}:s={W}x{H}:fps={fps},fade=t=in:st=0:d=0.4,fade=t=out:st={seg-0.4}:d=0.4,format=yuv420p")
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-loop", "1", "-i", str(OUT / nombre),
                    "-t", str(seg), "-vf", vf, "-c:v", "libx264", "-preset", "medium", "-crf", "20",
                    "-r", str(fps), str(clip)], check=True)
    clips.append(clip)
(OUT / "lista.txt").write_text("".join(f"file '{c.name}'\n" for c in clips))
salida = V / "Lumina Bank - Demo end-to-end.mp4"
subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0", "-i", str(OUT / "lista.txt"),
                "-c", "copy", "-movflags", "+faststart", str(salida)], check=True)
print(salida, sum(s for _, s in secuencia), "s")
