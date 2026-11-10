"""Convierte las salidas guardadas en logs/*.log en capturas PNG con estilo de terminal.

Uso (desde doc/evidencias/):  python3 render.py            # todas
                              python3 render.py 21_silver   # solo las que empiezan así
Las imágenes quedan en ../img/evidencias/. Se renderizan con Google Chrome en modo headless.
"""
import html
import re
import subprocess
import sys
import textwrap
from pathlib import Path

AQUI = Path(__file__).parent
SALIDA = AQUI.parent / "img" / "evidencias"
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
ANCHO_COL, MAX_LINEAS = 128, 70
# Algunos logs son largos: se recortan para que la captura sea legible (el log completo queda en logs/)
RECORTE = {"04_setup": 27, "24_simulador": 16, "08_gobierno_script": 40, "33_api": 44, "19_dataproc_submit": 22}
RUIDO = re.compile(r"(Waiting on bqjob|Current status: RUNNING|^\.{5,}$|^\s*$\n?^\s*$)")


def limpiar(texto: str, maximo: int = MAX_LINEAS) -> list[str]:
    lineas = []
    for linea in texto.replace("\r", "\n").splitlines():
        if "Waiting on bqjob" in linea or re.fullmatch(r"\.{3,}", linea.strip()):
            continue
        linea = re.sub(r"\.{8,}", "...", linea)
        envueltas = textwrap.wrap(linea, ANCHO_COL, replace_whitespace=False, drop_whitespace=False) or [""]
        lineas.extend(envueltas)
    while lineas and not lineas[-1].strip():
        lineas.pop()
    if len(lineas) > maximo:
        lineas = lineas[:maximo - 2] + ["", f"[… {len(lineas) - maximo + 2} líneas más en el log …]"]
    return lineas


def colorear(linea: str) -> str:
    t = html.escape(linea)
    if linea.startswith("$ "):
        return f'<span class="cmd">{t}</span>'
    if linea.startswith("# "):
        return f'<span class="meta">{t}</span>'
    if re.search(r"ERROR|error", linea):
        return f'<span class="err">{t}</span>'
    if re.search(r"Created|Replaced|successfully|Listo|Done|código de salida: 0|ENABLED|RUNNING|SUCCEEDED", linea):
        return f'<span class="ok">{t}</span>'
    return t


def renderizar(log: Path):
    lineas = limpiar(log.read_text(encoding="utf-8", errors="replace"), RECORTE.get(log.stem, MAX_LINEAS))
    cuerpo = "\n".join(colorear(l) for l in lineas)
    alto = 46 + 17 * len(lineas) + 26
    pagina = f"""<html><head><meta charset="utf-8"><style>
      body {{ margin:0; background:#fff; }}
      .win {{ width:1180px; background:#1E2230; border-radius:10px; overflow:hidden;
              font-family: Menlo, 'SF Mono', Consolas, monospace; }}
      .bar {{ height:30px; background:#2C3244; display:flex; align-items:center; padding-left:12px; gap:7px; }}
      .dot {{ width:11px; height:11px; border-radius:50%; }}
      .tit {{ color:#AEB6C8; font: 12px -apple-system, Arial; margin-left:14px; }}
      pre {{ margin:0; padding:10px 16px 14px; color:#E3E7EF; font-size:12.4px; line-height:17px; white-space:pre; }}
      .cmd {{ color:#7FD1B9; font-weight:bold; }} .meta {{ color:#8A93A8; }}
      .ok {{ color:#9BE07C; }} .err {{ color:#FF8A80; }}
    </style></head><body><div class="win"><div class="bar">
      <span class="dot" style="background:#FF5F57"></span><span class="dot" style="background:#FEBC2E"></span>
      <span class="dot" style="background:#28C840"></span>
      <span class="tit">Terminal · gcloud / bq · proyecto lumina-bank-utec</span></div>
      <pre>{cuerpo}</pre></div></body></html>"""
    tmp = SALIDA / f"_{log.stem}.html"
    tmp.write_text(pagina, encoding="utf-8")
    png = SALIDA / f"{log.stem}.png"
    subprocess.run([CHROME, "--headless", "--disable-gpu", "--hide-scrollbars", f"--window-size=1180,{alto}",
                    "--force-device-scale-factor=2", f"--screenshot={png}", tmp.as_uri()],
                   check=True, capture_output=True)
    tmp.unlink()
    print("OK", png.name, len(lineas), "líneas")


if __name__ == "__main__":
    SALIDA.mkdir(parents=True, exist_ok=True)
    filtro = sys.argv[1] if len(sys.argv) > 1 else ""
    for log in sorted((AQUI / "logs").glob(f"{filtro}*.log")):
        renderizar(log)
