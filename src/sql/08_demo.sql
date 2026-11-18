-- =====================================================================
-- 08_demo.sql — Consultas de la demo final (pegar en la consola de BigQuery, una por escena)
-- Ver el libreto en doc/demo/Libreto demo final.md
-- =====================================================================

-- Escena 2 · El histórico ya procesado por Spark (batch)
SELECT origen, COUNT(*) AS transacciones, MIN(fecha) AS desde, MAX(fecha) AS hasta
FROM `lumina-bank-utec.lumina_gold.fact_transacciones`
GROUP BY origen;

-- Escena 5 · Decisiones del pipeline en los últimos 10 minutos (re-ejecutar para ver cómo crecen)
SELECT decision, COUNT(*) AS transacciones,
       APPROX_QUANTILES(latencia_ms, 100)[OFFSET(95)] AS p95_ms
FROM `lumina-bank-utec.lumina_silver.transacciones_stream`
WHERE ingest_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
GROUP BY decision
ORDER BY transacciones DESC;

-- Escena 5 · Latencia por minuto: la meta es p95 < 5 000 ms
SELECT FORMAT_TIMESTAMP('%H:%M', decision_ts, 'America/Lima') AS minuto,
       COUNT(*) AS eventos,
       APPROX_QUANTILES(latencia_ms, 100)[OFFSET(95)] AS p95_ms
FROM `lumina-bank-utec.lumina_silver.transacciones_stream`
WHERE ingest_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
GROUP BY minuto
ORDER BY minuto;

-- Escena 6 · Alertas con las reglas que las dispararon
SELECT FORMAT_TIMESTAMP('%H:%M:%S', event_ts, 'America/Lima') AS hora, tipo,
       ROUND(monto) AS monto, distrito_id AS distrito, score, decision,
       ARRAY_TO_STRING(reglas, ', ') AS reglas
FROM `lumina-bank-utec.lumina_silver.alertas_fraude`
WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
ORDER BY event_ts DESC
LIMIT 15;

-- Escena 8 · Plan de efectivo T+1 por distrito (BigQuery ML, ARIMA_PLUS)
SELECT distrito, ROUND(efectivo_esperado / 1e6, 1) AS esperado_millones,
       ROUND(efectivo_recomendado / 1e6, 1) AS recomendado_millones
FROM `lumina-bank-utec.lumina_gold.v_plan_liquidez_t1`
ORDER BY distrito_id;
