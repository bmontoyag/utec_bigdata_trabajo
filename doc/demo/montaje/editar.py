"""Edita la grabación real de pantalla (toma.mkv): corta las esperas, ordena por escena y agrega rótulos."""
import subprocess
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

V = Path(__file__).parent
TOMA, OUT = V / "toma.mkv", V / "clips"
OUT.mkdir(exist_ok=True)
W, H = 1920, 1080
F = "/System/Library/Fonts/Helvetica.ttc"
f_esc, f_tit, f_cap = (ImageFont.truetype(F, s) for s in (22, 34, 28))
ACC, TXT = (66, 133, 244, 255), (240, 242, 245, 255)

# Toda la pantalla sin la barra de menú ni el Dock; o solo la Terminal (derecha).
PANTALLA = "crop=1800:1062:0:38,scale=-2:1080,pad=1920:1080:(ow-iw)/2:0:color=0x0f1115"
TERMINAL = "crop=610:1062:1190:38,scale=-2:1080,pad=1920:1080:(ow-iw)/2:0:color=0x0f1115"

# (inicio, fin, recorte, escena, título, leyenda)
CORTES = [
    (2, 16, PANTALLA, 2, "El histórico en el Data Lake", "PaySim (492 MB) en la zona raw de Cloud Storage"),
    (17, 44, PANTALLA, 2, "Spark en Dataproc Serverless, dentro de la VPC", "Job batch terminado · subred lumina-datos-us-central1"),
    (47, 72, PANTALLA, 2, "El modelo estrella en BigQuery", "6,36 millones de transacciones batch + lo que llega por streaming"),
    (80, 110, PANTALLA, 3, "El pipeline de fraude en Dataflow", "Lee Pub/Sub, parsea, DLQ y etapas con estado · 1 worker sin IP pública"),
    (110, 126, PANTALLA, 4, "Arranca el simulador", "A la derecha: transacciones reales de PaySim publicadas a 20 por segundo"),
    (280, 308, PANTALLA, 4, "Pub/Sub recibe y entrega", "Mensajes sin confirmar en cero: Dataflow consume al ritmo que llegan"),
    (145, 172, PANTALLA, 5, "Dataflow procesando", "Los contadores de elementos por segundo se actualizan en cada etapa"),
    (176, 224, PANTALLA, 5, "Decisiones en BigQuery, en vivo", "Aprobar, revisar o bloquear · al re-ejecutar, los conteos crecen"),
    (226, 254, PANTALLA, 5, "Latencia por minuto", "p95 por debajo de 1 s con carga estable · meta < 5 s"),
    (256, 270, PANTALLA, 6, "Alertas y reglas que las dispararon", "Vaciado de cuenta, destino inconsistente, monto alto, viaje imposible"),
    (570, 591, TERMINAL, 7, "La API en Cloud Run", "Sin token: 403 · con token: alertas con cliente y destino seudonimizados"),
    (600, 624, PANTALLA, 7, "Métricas de la API", "Solicitudes 2xx y 4xx, latencia e instancias que vuelven a cero"),
    (350, 378, PANTALLA, 8, "Tablero en Data Studio", "KPI por origen, transacciones por entidad, alertas y plan de liquidez"),
    (380, 400, PANTALLA, 8, "Plan de efectivo T+1 por distrito", "Pronóstico de BigQuery ML (ARIMA_PLUS)"),
    (625, 641, PANTALLA, 9, "Apagado del job", "A la derecha: drain, termina lo que tiene en vuelo y se apaga"),
    (645, 666, PANTALLA, 9, "Job drenando", "Solo se paga el tiempo que corrió · en producción escala de 4 a 55 workers"),
]


def rotulo(i, escena, titulo, leyenda):
    im = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(im)
    d.rectangle((0, 0, W, 64), fill=(15, 17, 21, 225))
    d.text((32, 8), f"ESCENA {escena}", font=f_esc, fill=ACC)
    d.text((180, 12), titulo, font=f_tit, fill=TXT)
    d.rectangle((0, H - 60, W, H), fill=(15, 17, 21, 225))
    d.text((32, H - 46), leyenda, font=f_cap, fill=TXT)
    ruta = OUT / f"r{i:02d}.png"
    im.save(ruta)
    return ruta


clips = []
for i, (ini, fin, recorte, escena, titulo, leyenda) in enumerate(CORTES):
    r = rotulo(i, escena, titulo, leyenda)
    clip = OUT / f"c{i:02d}.mp4"
    dur = fin - ini
    filtro = (f"[0:v]{recorte},fps=30[v];[v][1:v]overlay=0:0,"
              f"fade=t=in:st=0:d=0.3,fade=t=out:st={dur-0.3}:d=0.3,format=yuv420p")
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-ss", str(ini), "-to", str(fin), "-i", str(TOMA),
                    "-i", str(r), "-filter_complex", filtro, "-an", "-c:v", "libx264", "-preset", "medium",
                    "-crf", "20", str(clip)], check=True)
    clips.append(clip)

portada, cierre = V.parent / "frames" / "00.mp4", V.parent / "frames" / "99.mp4"
lista = [portada, *clips, cierre]
(OUT / "lista.txt").write_text("".join(f"file '{c}'\n" for c in lista))
salida = V / "Lumina Bank - Demo end-to-end.mp4"
subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0", "-i", str(OUT / "lista.txt"),
                "-c:v", "libx264", "-preset", "medium", "-crf", "20", "-r", "30", "-pix_fmt", "yuv420p",
                "-movflags", "+faststart", str(salida)], check=True)
print(salida)
