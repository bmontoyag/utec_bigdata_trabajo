"""Reglas de enriquecimiento deterministas compartidas por simulador, Spark y SQL.

La misma fórmula existe en:
  - BigQuery: función `lumina_gold.fn_bucket` (sql/01_ddl.sql)
  - Spark:    `bucket()` en dataproc/batch_paysim_etl.py
  bucket(valor, salt, n) = int(primeros 8 hex de md5(salt + valor), 16) mod n
"""
import hashlib
import math

N_DISTRITOS = 8
N_ENTIDADES = 12
CENTRO_LAT, CENTRO_LON = -12.06, -77.04  # centro ficticio de DataLandia
TIPOS_EFECTIVO = ("CASH_IN", "CASH_OUT")


def bucket(valor: str, salt: str, n: int) -> int:
    return int(hashlib.md5((salt + valor).encode("utf-8")).hexdigest()[:8], 16) % n


def distrito_id(cliente_id: str) -> int:
    return bucket(cliente_id, "d", N_DISTRITOS) + 1


def entidad_id(cliente_id: str) -> int:
    return bucket(cliente_id, "e", N_ENTIDADES) + 1


def edad(cliente_id: str) -> int:
    return 18 + bucket(cliente_id, "a", 63)


def centro_distrito(d: int) -> tuple[float, float]:
    i = d - 1
    return CENTRO_LAT + 0.08 * (i % 4 - 1.5), CENTRO_LON + 0.10 * (i // 4 - 0.5)


def domicilio(cliente_id: str) -> tuple[float, float]:
    lat, lon = centro_distrito(distrito_id(cliente_id))
    return (lat + (bucket(cliente_id, "la", 1000) / 1000 - 0.5) * 0.04,
            lon + (bucket(cliente_id, "lo", 1000) / 1000 - 0.5) * 0.04)


def canal(cliente_id: str, tipo: str) -> str:
    """Adultos mayores prefieren atención humana; jóvenes, canales digitales."""
    mayor = edad(cliente_id) >= 60
    if tipo in TIPOS_EFECTIVO:
        return "AGENCIA" if mayor else "CAJERO"
    return "AGENCIA" if mayor and bucket(cliente_id, "c", 2) == 0 else "APP"


def distancia_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))
