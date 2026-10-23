-- =====================================================================
-- 01_ddl.sql — Datasets, Bronze y Silver (ubicación US para cruzar con datasets públicos)
-- Ejecutar: sed "s/PROJECT_ID/$PROJECT_ID/g" sql/01_ddl.sql | bq query --use_legacy_sql=false --location=US
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.lumina_bronze` OPTIONS (location = 'US', description = 'Bronze: datos crudos');
CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.lumina_silver` OPTIONS (location = 'US', description = 'Silver: datos limpios y enriquecidos');
CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.lumina_gold`   OPTIONS (location = 'US', description = 'Gold: modelo estrella y KPIs');
CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.lumina_ml`     OPTIONS (location = 'US', description = 'Modelos BigQuery ML y pronósticos');

-- Hash determinista compartido con Python (simulator/common.py) y Spark (dataproc/batch_paysim_etl.py)
CREATE OR REPLACE FUNCTION `PROJECT_ID.lumina_gold.fn_bucket`(valor STRING, salt STRING, n INT64) AS (
  MOD(CAST(CONCAT('0x', SUBSTR(TO_HEX(MD5(CONCAT(salt, valor))), 1, 8)) AS INT64), n)
);

-- ---------------------------------------------------------------- Bronze
-- PaySim crudo en el Data Lake (tabla externa: no se copia el dato)
CREATE OR REPLACE EXTERNAL TABLE `PROJECT_ID.lumina_bronze.paysim_raw` (
  step INT64, type STRING, amount FLOAT64, nameOrig STRING, oldbalanceOrg FLOAT64, newbalanceOrig FLOAT64,
  nameDest STRING, oldbalanceDest FLOAT64, newbalanceDest FLOAT64, isFraud INT64, isFlaggedFraud INT64
)
OPTIONS (format = 'CSV', uris = ['gs://PROJECT_ID-lumina-raw/paysim/*.csv'], skip_leading_rows = 1);

-- Telemetría: la escribe directamente la suscripción BigQuery de Pub/Sub (sin código).
-- Las columnas de metadatos las llena Pub/Sub (--write-metadata); event_ts llega como texto ISO.
CREATE TABLE IF NOT EXISTS `PROJECT_ID.lumina_bronze.telemetria_raw` (
  terminal_id STRING, distrito_id INT64, tipo_terminal STRING, event_ts STRING,
  cpu_pct FLOAT64, temp_c FLOAT64, latencia_ms INT64, efectivo_pct FLOAT64, estado STRING,
  subscription_name STRING, message_id STRING, publish_time TIMESTAMP, attributes STRING
)
PARTITION BY DATE(publish_time)
CLUSTER BY terminal_id
OPTIONS (partition_expiration_days = 90);

-- Precios de mercado (EDI vía Analytics Hub o CSV alternativo). Ver 03_gold.sql.
CREATE TABLE IF NOT EXISTS `PROJECT_ID.lumina_bronze.mercado_precios` (
  fecha DATE, simbolo STRING, precio_cierre NUMERIC, moneda STRING, fuente STRING
);

-- Feriados nacionales Perú 2026 (fuente F7)
CREATE OR REPLACE TABLE `PROJECT_ID.lumina_bronze.feriados` AS
SELECT * FROM UNNEST([
  STRUCT(DATE '2026-01-01' AS fecha, 'Año Nuevo' AS nombre),
  (DATE '2026-04-02', 'Jueves Santo'), (DATE '2026-04-03', 'Viernes Santo'),
  (DATE '2026-05-01', 'Día del Trabajo'), (DATE '2026-06-07', 'Batalla de Arica y Día de la Bandera'),
  (DATE '2026-06-29', 'San Pedro y San Pablo'), (DATE '2026-07-23', 'Día de la Fuerza Aérea'),
  (DATE '2026-07-28', 'Fiestas Patrias'), (DATE '2026-07-29', 'Fiestas Patrias'),
  (DATE '2026-08-06', 'Batalla de Junín'), (DATE '2026-08-30', 'Santa Rosa de Lima'),
  (DATE '2026-10-08', 'Combate de Angamos'), (DATE '2026-11-01', 'Todos los Santos'),
  (DATE '2026-12-08', 'Inmaculada Concepción'), (DATE '2026-12-09', 'Batalla de Ayacucho'),
  (DATE '2026-12-25', 'Navidad')
]);

-- ---------------------------------------------------------------- Silver
-- La escribe el pipeline Dataflow (fraud_streaming_pipeline.py)
CREATE TABLE IF NOT EXISTS `PROJECT_ID.lumina_silver.transacciones_stream` (
  event_id STRING NOT NULL, event_ts TIMESTAMP NOT NULL, ingest_ts TIMESTAMP, decision_ts TIMESTAMP,
  step INT64, tipo STRING, monto NUMERIC, cliente_id STRING,
  saldo_ant_orig NUMERIC, saldo_nuevo_orig NUMERIC, destino_id STRING, saldo_ant_dest NUMERIC, saldo_nuevo_dest NUMERIC,
  canal STRING, entidad_id INT64, distrito_id INT64, lat FLOAT64, lon FLOAT64, device_id STRING,
  is_fraud_label INT64, sintetico BOOL,
  velocidad_kmh FLOAT64, tx_destino_10min INT64,
  score FLOAT64, decision STRING, reglas ARRAY<STRING>, latencia_ms INT64
)
PARTITION BY DATE(event_ts)
CLUSTER BY distrito_id, tipo, cliente_id
OPTIONS (description = 'Transacciones en tiempo real con decisión de fraude');

CREATE TABLE IF NOT EXISTS `PROJECT_ID.lumina_silver.alertas_fraude` (
  event_id STRING, event_ts TIMESTAMP, decision_ts TIMESTAMP, cliente_id STRING, destino_id STRING,
  distrito_id INT64, entidad_id INT64, tipo STRING, monto NUMERIC,
  score FLOAT64, decision STRING, reglas ARRAY<STRING>, is_fraud_label INT64
)
PARTITION BY DATE(event_ts)
CLUSTER BY decision, distrito_id;

-- lumina_silver.transacciones_batch la crea `bq load` desde el Parquet curado (ver README, paso 4).
