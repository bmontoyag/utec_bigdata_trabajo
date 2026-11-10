"""Resume el resultado de un job de calidad de Dataplex (JSON por stdin)."""
import json
import sys

j = json.load(sys.stdin)
r = j.get("dataQualityResult", {})
print("job:", j["name"].split("/")[-1], "· estado:", j["state"], "· filas evaluadas:", r.get("rowCount"))
print("resultado global:", "APROBADO" if r.get("passed") else "CON FALLAS", "· puntaje:", r.get("score"))
print()
print("%-13s %-13s %-24s %10s %10s  %s" % ("columna", "dimensión", "regla", "evaluadas", "aprobadas", "resultado"))
for x in r.get("rules", []):
    regla = x["rule"]
    tipo = [k for k in regla if k.endswith("Expectation")][0]
    print("%-13s %-13s %-24s %10s %10s  %s" % (regla.get("column", ""), regla.get("dimension", ""), tipo,
          x.get("evaluatedCount", ""), x.get("passedCount", ""), "OK" if x.get("passed") else "FALLA"))
