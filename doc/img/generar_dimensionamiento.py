"""Gráficos de la prueba de carga y del dimensionamiento (Entrega 2). Requiere matplotlib.

Uso (desde doc/img/):  python3 generar_dimensionamiento.py
Lee ../evidencias/carga_por_minuto.csv (salida de BigQuery) y escribe carga.png y dimensionamiento.png.
"""
import csv
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

AQUI = Path(__file__).parent
NAVY, TEAL, BLUE, ORANGE, RED, GRAY = "#14213D", "#2A9D8F", "#3D5A80", "#F2A541", "#E76F51", "#6B7385"
COLORES = {"Corrida 1": RED, "Corrida 2": ORANGE, "Corrida 3": BLUE}
plt.rcParams.update({"font.family": "Arial", "font.size": 10.5, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.edgecolor": GRAY, "axes.labelcolor": NAVY,
                     "xtick.color": NAVY, "ytick.color": NAVY})

# ------------------------------------------------------------------ prueba de carga
series = defaultdict(list)
with open(AQUI.parent / "evidencias" / "carga_por_minuto.csv") as f:
    for fila in csv.DictReader(f):
        series[fila["corrida"]].append((datetime.fromisoformat(fila["minuto"]), int(fila["eventos"]) / 60,
                                        int(fila["p95_ms"]) / 1000))

fig, (a1, a2) = plt.subplots(1, 2, figsize=(12.5, 4.3), dpi=200)
for nombre, puntos in series.items():
    puntos = [p for p in puntos if p[1] > 8]          # fuera los minutos de arranque o cierre sin carga
    t0 = puntos[0][0]
    x = [(p[0] - t0).total_seconds() / 60 for p in puntos]
    a1.plot(x, [p[1] for p in puntos], marker="o", color=COLORES[nombre], lw=2, label=nombre)
    a2.plot(x, [p[2] for p in puntos], marker="o", color=COLORES[nombre], lw=2, label=nombre)
a1.axhline(100, color=GRAY, ls="--", lw=1.2)
a1.text(0.1, 104, "carga del primer escalón: 100 ev/s", color=GRAY, fontsize=9)
a1.set_title("Eventos procesados por segundo (1 worker n2-standard-2)", loc="left", color=NAVY, fontweight="bold")
a1.set_xlabel("minutos desde el inicio de la corrida")
a1.set_ylim(0, 160)
a2.axhline(5, color=TEAL, ls="--", lw=1.2)
a2.text(0.1, 5.8, "meta: p95 < 5 s", color=TEAL, fontsize=9)
a2.set_yscale("log")
a2.set_title("Latencia p95 de punta a punta (s, escala log)", loc="left", color=NAVY, fontweight="bold")
a2.set_xlabel("minutos desde el inicio de la corrida")
for a in (a1, a2):
    a.grid(axis="y", color="#E3E9F2", zorder=0)
    a.legend(frameon=False)
fig.tight_layout()
fig.savefig(AQUI / "carga.png", bbox_inches="tight", facecolor="white")

# ------------------------------------------------------------------ dimensionamiento
ESCENARIOS = [("Promedio\n175 TPS", 175), ("Pico normal\n440 TPS", 440),
              ("Día de pago\n2,000 TPS", 2_000), ("Diseño\n5,000 TPS", 5_000)]
UTIL = 0.70
MEDIDO = 75          # ev/s sostenidos por worker en las pruebas (código actual en Python)
OBJETIVO = 500       # ev/s por worker que debe alcanzar la versión optimizada antes de producción
RPS_INSTANCIA = 500 * 0.60


def workers(tps, capacidad):
    return max(1, math.ceil(tps / (capacidad * UTIL)))


fig, (b1, b2) = plt.subplots(1, 2, figsize=(12.5, 4.5), dpi=200, gridspec_kw={"width_ratios": [1.25, 1]})
etiquetas = [e for e, _ in ESCENARIOS]
xs = range(len(ESCENARIOS))
med = [workers(t, MEDIDO) for _, t in ESCENARIOS]
obj = [workers(t, OBJETIVO) for _, t in ESCENARIOS]
ancho = 0.38
b1.bar([x - ancho / 2 for x in xs], med, ancho, color=RED, zorder=3, label=f"código actual ({MEDIDO} ev/s por worker, medido)")
b1.bar([x + ancho / 2 for x in xs], obj, ancho, color=BLUE, zorder=3, label=f"versión optimizada ({OBJETIVO} ev/s, objetivo)")
for x, a, b in zip(xs, med, obj):
    b1.text(x - ancho / 2, a + 2, str(a), ha="center", color=NAVY, fontweight="bold")
    b1.text(x + ancho / 2, b + 2, str(b), ha="center", color=NAVY, fontweight="bold")
b1.axhline(30, color=GRAY, ls="--", lw=1.2)
b1.text(-0.45, 32, "máximo de la Entrega 1 (30, sin medir)", color=GRAY, fontsize=9)
b1.set_xticks(list(xs), etiquetas)
b1.set_ylim(0, 115)
b1.set_title("Dataflow: workers n2-standard-2 necesarios al 70 % de uso", loc="left", color=NAVY, fontweight="bold")
b1.legend(frameon=False, loc="upper left", bbox_to_anchor=(0, 0.93))

inst = [max(2, math.ceil(t / RPS_INSTANCIA)) for _, t in ESCENARIOS]
barras = b2.bar(etiquetas, inst, color=TEAL, width=0.55, zorder=3)
for b, v in zip(barras, inst):
    b2.text(b.get_x() + b.get_width() / 2, v + 0.5, str(v), ha="center", color=NAVY, fontweight="bold")
b2.axhline(20, color=ORANGE, lw=2)
b2.text(3.45, 20.6, "máximo nuevo = 20", ha="right", color="#B9741A", fontweight="bold")
b2.text(-0.4, 25.5, "Entrega 1: máximo 100", color=RED, fontsize=9.5)
b2.set_ylim(0, 27)
b2.set_title("Cloud Run (autorización): instancias", loc="left", color=NAVY, fontweight="bold")
for a in (b1, b2):
    a.grid(axis="y", color="#E3E9F2", zorder=0)
fig.tight_layout()
fig.savefig(AQUI / "dimensionamiento.png", bbox_inches="tight", facecolor="white")
for (_, t), a, b, c in zip(ESCENARIOS, med, obj, inst):
    print(f"{t:>5} TPS → Dataflow actual {a} · optimizado {b} · Cloud Run {c}")
