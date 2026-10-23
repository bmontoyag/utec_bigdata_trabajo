-- =====================================================================
-- 02_dimensiones.sql — Dimensiones conformadas (Gold)
-- Requiere: 01_ddl.sql ejecutado y PaySim subido a gs://PROJECT_ID-lumina-raw/paysim/
-- Escaneo aprox.: PaySim ~0.5 GB, SEC quick_summary ~5-8 GB, Trends < 1 GB (dentro del free tier de 1 TiB/mes)
-- =====================================================================

-- dim_distrito: 8 distritos de DataLandia, cada uno asociado a una región de Perú en Google Trends.
-- La asignación (orden alfabético de regiones) es arbitraria pero reproducible.
CREATE OR REPLACE TABLE `PROJECT_ID.lumina_gold.dim_distrito` AS
WITH regiones AS (
  SELECT DISTINCT region_name, region_code
  FROM `bigquery-public-data.google_trends.international_top_terms`
  WHERE country_code = 'PE'
    AND refresh_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
),
numeradas AS (
  SELECT ROW_NUMBER() OVER (ORDER BY region_name) AS distrito_id, region_name, region_code
  FROM regiones
)
SELECT
  distrito_id,
  ['Centro Financiero', 'Norte Digital', 'Sur Residencial', 'Este Industrial',
   'Oeste Portuario', 'Valle Comercial', 'Altos Residencial', 'Bahía Empresarial'][OFFSET(distrito_id - 1)] AS nombre,
  IF(distrito_id IN (3, 7), 'RESIDENCIAL', 'MIXTO') AS tipo_zona,
  region_name AS region_trends,
  region_code AS region_trends_code,
  -12.06 + 0.08 * (MOD(distrito_id - 1, 4) - 1.5) AS centro_lat,
  -77.04 + 0.10 * (DIV(distrito_id - 1, 4) - 0.5) AS centro_lon
FROM numeradas
WHERE distrito_id <= 8;

-- dim_entidad: las 12 entidades de Lumina. A cada una se le asigna como proxy financiero
-- uno de los 12 bancos comerciales (SIC 6021/6022) con más activos en la SEC (FY2020).
CREATE OR REPLACE TABLE `PROJECT_ID.lumina_gold.dim_entidad` AS
WITH bancos AS (
  SELECT central_index_key AS cik, ANY_VALUE(company_name) AS banco_proxy, MAX(value) AS activos_usd
  FROM `bigquery-public-data.sec_quarterly_financials.quick_summary`
  WHERE sic IN ('6021', '6022') AND form = '10-K' AND fiscal_year = 2020
    AND measure_tag = 'Assets' AND number_of_quarters = 0
  GROUP BY cik
)
SELECT
  ROW_NUMBER() OVER (ORDER BY activos_usd DESC) AS entidad_id,
  FORMAT('Entidad Lumina %02d', ROW_NUMBER() OVER (ORDER BY activos_usd DESC)) AS nombre,
  cik AS cik_proxy, banco_proxy, activos_usd
FROM bancos
QUALIFY entidad_id <= 12;

-- dim_cliente: ~9 M clientes (originantes + destinos personas de PaySim) con atributos deterministas
CREATE OR REPLACE TABLE `PROJECT_ID.lumina_gold.dim_cliente`
CLUSTER BY cliente_id AS
WITH clientes AS (
  SELECT nameOrig AS cliente_id FROM `PROJECT_ID.lumina_bronze.paysim_raw`
  UNION DISTINCT
  SELECT nameDest FROM `PROJECT_ID.lumina_bronze.paysim_raw` WHERE STARTS_WITH(nameDest, 'C')
),
base AS (
  SELECT
    cliente_id,
    `PROJECT_ID.lumina_gold.fn_bucket`(cliente_id, 'd', 8) + 1 AS distrito_id,
    `PROJECT_ID.lumina_gold.fn_bucket`(cliente_id, 'e', 12) + 1 AS entidad_id,
    18 + `PROJECT_ID.lumina_gold.fn_bucket`(cliente_id, 'a', 63) AS edad,
    `PROJECT_ID.lumina_gold.fn_bucket`(cliente_id, 'la', 1000) / 1000 - 0.5 AS off_lat,
    `PROJECT_ID.lumina_gold.fn_bucket`(cliente_id, 'lo', 1000) / 1000 - 0.5 AS off_lon
  FROM clientes
)
SELECT
  cliente_id, distrito_id, entidad_id, edad,
  CASE WHEN edad < 30 THEN '18-29 Joven digital' WHEN edad < 60 THEN '30-59 Adulto' ELSE '60+ Adulto mayor' END AS segmento_etario,
  IF(edad >= 60, 'ATENCION_HUMANA', 'DIGITAL') AS preferencia_canal,
  ROUND(-12.06 + 0.08 * (MOD(distrito_id - 1, 4) - 1.5) + off_lat * 0.04, 3) AS domicilio_lat,  -- redondeo = anonimización
  ROUND(-77.04 + 0.10 * (DIV(distrito_id - 1, 4) - 0.5) + off_lon * 0.04, 3) AS domicilio_lon
FROM base;

-- dim_fecha: calendario 2026-2027 con feriados y días de pago (15 y fin de mes)
CREATE OR REPLACE TABLE `PROJECT_ID.lumina_gold.dim_fecha` AS
SELECT
  d AS fecha,
  EXTRACT(YEAR FROM d) AS anio, EXTRACT(MONTH FROM d) AS mes, EXTRACT(DAY FROM d) AS dia,
  FORMAT_DATE('%A', d) AS dia_semana, EXTRACT(ISOWEEK FROM d) AS semana_iso,
  DATE_TRUNC(d, WEEK(SUNDAY)) AS semana_inicio,
  EXTRACT(DAYOFWEEK FROM d) IN (1, 7) AS es_fin_semana,
  f.fecha IS NOT NULL AS es_feriado, f.nombre AS feriado,
  (EXTRACT(DAY FROM d) = 15 OR d = LAST_DAY(d)) AS es_dia_pago
FROM UNNEST(GENERATE_DATE_ARRAY('2026-01-01', '2027-12-31')) AS d
LEFT JOIN `PROJECT_ID.lumina_bronze.feriados` f ON f.fecha = d;

-- dim_tipo_transaccion: comisiones de referencia (SUPUESTO de negocio) para estimar ingreso por producto
CREATE OR REPLACE TABLE `PROJECT_ID.lumina_gold.dim_tipo_transaccion` AS
SELECT * FROM UNNEST([
  STRUCT('CASH_IN' AS tipo, 'Depósito en efectivo' AS descripcion, 'Efectivo' AS producto,
         NUMERIC '0' AS comision_pct, NUMERIC '0' AS comision_fija, FALSE AS es_salida_efectivo),
  ('CASH_OUT', 'Retiro en efectivo', 'Efectivo', NUMERIC '0.005', NUMERIC '1.00', TRUE),
  ('DEBIT',    'Débito a cuenta', 'Cuentas', NUMERIC '0.002', NUMERIC '0', FALSE),
  ('PAYMENT',  'Pago a comercio', 'Pagos', NUMERIC '0.010', NUMERIC '0', FALSE),
  ('TRANSFER', 'Transferencia', 'Transferencias', NUMERIC '0', NUMERIC '0.50', FALSE)
]);
