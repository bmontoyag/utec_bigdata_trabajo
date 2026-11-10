-- =====================================================================
-- 06_validacion_capacidad.sql — Validación del dimensionamiento de Dataflow (prueba de carga)
-- Ejecutar DESPUÉS de cada escalón de la prueba (README, paso 10). latencia_ms = decision_ts − enviado_ts,
-- así que incluye la espera en Pub/Sub: si el pipeline se satura, el backlog crece y la latencia sube.
-- =====================================================================

DECLARE desde TIMESTAMP DEFAULT TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR);

-- 1) Throughput y latencia por minuto (capturar el resultado como evidencia)
SELECT TIMESTAMP_TRUNC(decision_ts, MINUTE) AS minuto,
       COUNT(*) AS eventos,
       ROUND(COUNT(*) / 60, 1) AS ev_por_s,
       APPROX_QUANTILES(latencia_ms, 100)[OFFSET(50)] AS p50_ms,
       APPROX_QUANTILES(latencia_ms, 100)[OFFSET(95)] AS p95_ms,
       APPROX_QUANTILES(latencia_ms, 100)[OFFSET(99)] AS p99_ms,
       COUNTIF(decision != 'APROBAR') AS alertas
FROM `PROJECT_ID.lumina_silver.transacciones_stream`
WHERE event_ts >= desde
GROUP BY minuto
ORDER BY minuto;

-- 2) Capacidad sostenida: mayor throughput por minuto con p95 < 5 s (criterio de aceptación, E1 §12.3)
SELECT MAX(ev_por_s) AS capacidad_sostenida_ev_s,
       -- workers necesarios para el pico de diseño (5,000 TPS) al 70 % de uso objetivo
       CEIL(5000 / (0.7 * MAX(ev_por_s) / ANY_VALUE(workers))) AS max_num_workers_recomendado
FROM (
  SELECT TIMESTAMP_TRUNC(decision_ts, MINUTE) AS minuto,
         COUNT(*) / 60 AS ev_por_s,
         APPROX_QUANTILES(latencia_ms, 100)[OFFSET(95)] AS p95_ms,
         1 AS workers  -- la prueba se corre con el job fijo en 1 worker (--max_num_workers 1)
  FROM `PROJECT_ID.lumina_silver.transacciones_stream`
  WHERE event_ts >= desde
  GROUP BY minuto
)
WHERE p95_ms < 5000;

-- 3) Integridad: ningún evento duplicado ni perdido (comparar con lo que reportó el simulador)
SELECT COUNT(*) AS filas, COUNT(DISTINCT event_id) AS eventos_unicos,
       COUNT(*) - COUNT(DISTINCT event_id) AS duplicados
FROM `PROJECT_ID.lumina_silver.transacciones_stream`
WHERE event_ts >= desde;
