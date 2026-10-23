-- =====================================================================
-- 03_gold.sql — Hechos, vistas de KPIs y cruce multifuente (Gold)
-- Requiere: 02_dimensiones.sql y lumina_silver.transacciones_batch cargada (README, paso 4)
-- =====================================================================

DECLARE ultima_trends DATE DEFAULT (
  SELECT MAX(refresh_date) FROM `bigquery-public-data.google_trends.international_top_terms`
  WHERE refresh_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND country_code = 'PE'
);

-- ---------------------------------------------------------------- Hechos
-- Vista unificada batch + streaming (mismo esquema → mismas métricas)
CREATE OR REPLACE VIEW `PROJECT_ID.lumina_gold.fact_transacciones` AS
SELECT event_id, event_ts, DATE(event_ts) AS fecha, tipo, monto, cliente_id, destino_id, canal,
       entidad_id, distrito_id, lat, lon, is_fraud_label,
       CAST(NULL AS FLOAT64) AS score, CAST(NULL AS STRING) AS decision, CAST(NULL AS INT64) AS latencia_ms,
       'batch' AS origen
FROM `PROJECT_ID.lumina_silver.transacciones_batch`
UNION ALL
SELECT event_id, event_ts, DATE(event_ts), tipo, monto, cliente_id, destino_id, canal,
       entidad_id, distrito_id, lat, lon, is_fraud_label,
       score, decision, latencia_ms,
       'stream'
FROM `PROJECT_ID.lumina_silver.transacciones_stream`
WHERE NOT IFNULL(sintetico, FALSE);

-- Liquidez: flujo de efectivo por distrito y hora (serie completa, horas sin movimiento = 0)
CREATE OR REPLACE TABLE `PROJECT_ID.lumina_gold.fact_liquidez_hora`
PARTITION BY DATE(hora) CLUSTER BY distrito_id AS
WITH rango AS (
  SELECT MIN(TIMESTAMP_TRUNC(event_ts, HOUR)) AS ini, MAX(TIMESTAMP_TRUNC(event_ts, HOUR)) AS fin
  FROM `PROJECT_ID.lumina_silver.transacciones_batch`
),
horas AS (
  SELECT h AS hora, d.distrito_id
  FROM rango, UNNEST(GENERATE_TIMESTAMP_ARRAY(ini, fin, INTERVAL 1 HOUR)) AS h
  CROSS JOIN `PROJECT_ID.lumina_gold.dim_distrito` AS d
),
agg AS (
  SELECT TIMESTAMP_TRUNC(event_ts, HOUR) AS hora, distrito_id,
         SUM(IF(tipo = 'CASH_OUT', monto, 0)) AS salidas_efectivo,
         SUM(IF(tipo = 'CASH_IN', monto, 0)) AS entradas_efectivo,
         COUNT(*) AS n_tx_efectivo
  FROM `PROJECT_ID.lumina_gold.fact_transacciones`
  WHERE tipo IN ('CASH_OUT', 'CASH_IN')
  GROUP BY 1, 2
)
SELECT h.hora, h.distrito_id,
       IFNULL(a.salidas_efectivo, 0) AS salidas_efectivo,
       IFNULL(a.entradas_efectivo, 0) AS entradas_efectivo,
       IFNULL(a.entradas_efectivo, 0) - IFNULL(a.salidas_efectivo, 0) AS flujo_neto,
       IFNULL(a.n_tx_efectivo, 0) AS n_tx_efectivo,
       f.es_feriado, f.es_dia_pago, f.es_fin_semana
FROM horas h
LEFT JOIN agg a USING (hora, distrito_id)
LEFT JOIN `PROJECT_ID.lumina_gold.dim_fecha` f ON f.fecha = DATE(h.hora);

-- Google Trends: intensidad de búsquedas financieras por distrito y semana
CREATE OR REPLACE TABLE `PROJECT_ID.lumina_gold.fact_tendencias_semana` AS
SELECT d.distrito_id, t.week AS semana,
       COUNT(*) AS terminos_top,
       COUNTIF(REGEXP_CONTAINS(LOWER(t.term),
         r'banco|pr[eé]stamo|cr[eé]dito|bono|afp|cts|tarjeta|yape|plin|retiro|gratificaci|bcp|bbva|interbank|scotiabank|sunat|d[oó]lar')) AS terminos_financieros,
       AVG(t.score) AS score_promedio,
       ARRAY_AGG(IF(REGEXP_CONTAINS(LOWER(t.term),
         r'banco|pr[eé]stamo|cr[eé]dito|bono|afp|cts|tarjeta|yape|plin|retiro|gratificaci|bcp|bbva|interbank|scotiabank|sunat|d[oó]lar'),
         t.term, NULL) IGNORE NULLS LIMIT 10) AS ejemplos_financieros
FROM `bigquery-public-data.google_trends.international_top_terms` t
JOIN `PROJECT_ID.lumina_gold.dim_distrito` d
  ON t.region_name = d.region_trends
WHERE t.refresh_date = ultima_trends AND t.country_code = 'PE'
GROUP BY 1, 2;

-- SEC: estados financieros de bancos comerciales (benchmark del sector) y proxies de las 12 entidades
CREATE OR REPLACE TABLE `PROJECT_ID.lumina_gold.fact_benchmark_sector` AS
WITH base AS (
  SELECT central_index_key AS cik, ANY_VALUE(company_name) AS empresa, fiscal_year, measure_tag,
         ARRAY_AGG(value ORDER BY period_end_date DESC, date_filed DESC LIMIT 1)[OFFSET(0)] AS valor
  FROM `bigquery-public-data.sec_quarterly_financials.quick_summary`
  WHERE sic IN ('6021', '6022') AND form = '10-K' AND fiscal_period_focus = 'FY'
    AND fiscal_year BETWEEN 2015 AND 2020 AND number_of_quarters IN (0, 4)
    AND measure_tag IN ('Assets', 'Liabilities', 'StockholdersEquity', 'NetIncomeLoss', 'InterestExpense')
  GROUP BY cik, fiscal_year, measure_tag
),
pivot AS (
  SELECT cik, ANY_VALUE(empresa) AS empresa, fiscal_year AS anio_fiscal,
         MAX(IF(measure_tag = 'Assets', valor, NULL)) AS activos,
         MAX(IF(measure_tag = 'Liabilities', valor, NULL)) AS pasivos,
         MAX(IF(measure_tag = 'StockholdersEquity', valor, NULL)) AS patrimonio,
         MAX(IF(measure_tag = 'NetIncomeLoss', valor, NULL)) AS utilidad_neta,
         MAX(IF(measure_tag = 'InterestExpense', valor, NULL)) AS gasto_intereses
  FROM base GROUP BY cik, fiscal_year
)
SELECT p.*,
       SAFE_DIVIDE(utilidad_neta, activos) AS roa,
       SAFE_DIVIDE(utilidad_neta, patrimonio) AS roe,
       SAFE_DIVIDE(pasivos, activos) AS apalancamiento,
       e.entidad_id,
       e.entidad_id IS NOT NULL AS es_proxy_lumina
FROM pivot p
LEFT JOIN `PROJECT_ID.lumina_gold.dim_entidad` e ON e.cik_proxy = p.cik;

-- Mercado (EDI). Poblar lumina_bronze.mercado_precios desde el dataset enlazado de Analytics Hub, p. ej.:
--   INSERT INTO `PROJECT_ID.lumina_bronze.mercado_precios`
--   SELECT <fecha>, <simbolo>, CAST(<cierre> AS NUMERIC), <moneda>, 'EDI' FROM `PROJECT_ID.<dataset_enlazado_edi>.<tabla>`;
-- (los nombres de columna dependen del listing suscrito) o con `bq load` de un CSV alternativo.
CREATE OR REPLACE VIEW `PROJECT_ID.lumina_gold.fact_mercado_diario` AS
SELECT fecha, simbolo, moneda, fuente, precio_cierre,
       SAFE_DIVIDE(precio_cierre - LAG(precio_cierre) OVER w, LAG(precio_cierre) OVER w) AS retorno_diario
FROM `PROJECT_ID.lumina_bronze.mercado_precios`
WINDOW w AS (PARTITION BY simbolo ORDER BY fecha);

-- ---------------------------------------------------------------- Vistas de KPIs (Vertical III)
CREATE OR REPLACE VIEW `PROJECT_ID.lumina_gold.v_kpi_operativos_diario` AS
SELECT t.fecha, t.entidad_id, t.origen,
       COUNT(*) AS n_tx,
       SUM(t.monto) AS monto_total,
       SUM(t.monto * c.comision_pct + c.comision_fija) AS ingreso_estimado,
       AVG(t.is_fraud_label) AS tasa_fraude_real,
       SAFE_DIVIDE(COUNTIF(t.decision = 'BLOQUEAR'), COUNTIF(t.decision IS NOT NULL)) AS tasa_bloqueo,
       AVG(t.latencia_ms) AS tiempo_aprobacion_prom_ms,
       APPROX_QUANTILES(t.latencia_ms, 100 IGNORE NULLS)[SAFE_OFFSET(95)] AS tiempo_aprobacion_p95_ms
FROM `PROJECT_ID.lumina_gold.fact_transacciones` t
JOIN `PROJECT_ID.lumina_gold.dim_tipo_transaccion` c USING (tipo)
GROUP BY 1, 2, 3;

CREATE OR REPLACE VIEW `PROJECT_ID.lumina_gold.v_ingreso_por_producto` AS
SELECT t.fecha, c.producto, t.tipo, COUNT(*) AS n_tx,
       SUM(t.monto * c.comision_pct + c.comision_fija) AS ingreso_estimado
FROM `PROJECT_ID.lumina_gold.fact_transacciones` t
JOIN `PROJECT_ID.lumina_gold.dim_tipo_transaccion` c USING (tipo)
GROUP BY 1, 2, 3;

-- Desempeño de las reglas streaming contra la etiqueta PaySim (solo laboratorio)
CREATE OR REPLACE VIEW `PROJECT_ID.lumina_gold.v_desempeno_reglas_fraude` AS
WITH m AS (
  SELECT COUNTIF(decision != 'APROBAR' AND is_fraud_label = 1) AS vp,
         COUNTIF(decision != 'APROBAR' AND is_fraud_label = 0) AS fp,
         COUNTIF(decision = 'APROBAR' AND is_fraud_label = 1) AS fn,
         COUNTIF(decision = 'APROBAR' AND is_fraud_label = 0) AS vn
  FROM `PROJECT_ID.lumina_silver.transacciones_stream`
  WHERE NOT IFNULL(sintetico, FALSE)
)
SELECT *, SAFE_DIVIDE(vp, vp + fp) AS precision, SAFE_DIVIDE(vp, vp + fn) AS recall FROM m;

CREATE OR REPLACE VIEW `PROJECT_ID.lumina_gold.v_salud_financiera_vs_sector` AS
WITH sector AS (
  SELECT anio_fiscal,
         APPROX_QUANTILES(roa, 2 IGNORE NULLS)[OFFSET(1)] AS roa_mediana_sector,
         APPROX_QUANTILES(apalancamiento, 2 IGNORE NULLS)[OFFSET(1)] AS apalancamiento_mediana_sector,
         COUNT(DISTINCT cik) AS bancos_sector
  FROM `PROJECT_ID.lumina_gold.fact_benchmark_sector`
  WHERE activos > 0
  GROUP BY 1
),
lumina AS (
  SELECT anio_fiscal,
         SAFE_DIVIDE(SUM(utilidad_neta), SUM(activos)) AS roa_lumina,
         SAFE_DIVIDE(SUM(pasivos), SUM(activos)) AS apalancamiento_lumina
  FROM `PROJECT_ID.lumina_gold.fact_benchmark_sector`
  WHERE es_proxy_lumina
  GROUP BY 1
)
SELECT * FROM sector JOIN lumina USING (anio_fiscal);

-- Vertical IV: congestión transaccional por distrito y hora del día
CREATE OR REPLACE VIEW `PROJECT_ID.lumina_gold.v_congestion_distrito_hora` AS
SELECT d.nombre AS distrito, t.distrito_id,
       EXTRACT(DAYOFWEEK FROM t.event_ts) AS dia_semana, EXTRACT(HOUR FROM t.event_ts) AS hora_dia,
       COUNT(*) AS n_tx,
       RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking_congestion
FROM `PROJECT_ID.lumina_gold.fact_transacciones` t
JOIN `PROJECT_ID.lumina_gold.dim_distrito` d USING (distrito_id)
GROUP BY 1, 2, 3, 4;

-- Vertical IV: salud de terminales (últimos 15 minutos)
CREATE OR REPLACE VIEW `PROJECT_ID.lumina_gold.v_salud_terminales` AS
SELECT terminal_id, ANY_VALUE(distrito_id) AS distrito_id, ANY_VALUE(tipo_terminal) AS tipo_terminal,
       MAX(SAFE_CAST(event_ts AS TIMESTAMP)) AS ultima_lectura,
       AVG(latencia_ms) AS latencia_prom_ms, MAX(temp_c) AS temp_max_c,
       ARRAY_AGG(efectivo_pct ORDER BY publish_time DESC LIMIT 1)[OFFSET(0)] AS efectivo_pct,
       COUNTIF(estado != 'OK') AS lecturas_con_falla,
       CASE WHEN COUNTIF(estado = 'ERROR') > 0 THEN 'CRITICO'
            WHEN AVG(latencia_ms) > 600 OR MIN(efectivo_pct) < 20 THEN 'ALERTA'
            ELSE 'OK' END AS semaforo
FROM `PROJECT_ID.lumina_bronze.telemetria_raw`
WHERE publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
GROUP BY terminal_id;

-- Costo operativo REAL por transacción (habilitar Billing export a BigQuery y reemplazar la tabla):
-- CREATE OR REPLACE VIEW `PROJECT_ID.lumina_gold.v_kpi_costo_por_tx` AS
-- WITH costo AS (
--   SELECT DATE(usage_start_time) AS fecha, SUM(cost) AS costo_usd
--   FROM `PROJECT_ID.billing_export.gcp_billing_export_v1_XXXXXX` WHERE project.id = 'PROJECT_ID' GROUP BY 1),
-- tx AS (SELECT DATE(ingest_ts) AS fecha, COUNT(*) AS n_tx FROM `PROJECT_ID.lumina_silver.transacciones_stream` GROUP BY 1)
-- SELECT fecha, costo_usd, n_tx, SAFE_DIVIDE(costo_usd, n_tx) * 1e6 AS usd_por_millon_tx
-- FROM costo JOIN tx USING (fecha);
