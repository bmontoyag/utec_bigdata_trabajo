-- =====================================================================
-- 04_bqml.sql — Modelos BigQuery ML: fraude (Vertical I) y liquidez (Vertical II)
-- =====================================================================

-- ---------------------------------------------------------------- Fraude (XGBoost)
-- Solo TRANSFER/CASH_OUT (en PaySim el fraude solo ocurre ahí). Split determinista 80/20 por event_id.
-- Entrenamiento: todos los fraudes + 10 % de no fraudes (submuestreo para costo/tiempo).
-- Sin variables geográficas para evitar fuga de etiqueta (la geo del simulador depende de isFraud).
CREATE OR REPLACE MODEL `PROJECT_ID.lumina_ml.modelo_fraude`
OPTIONS (
  model_type = 'BOOSTED_TREE_CLASSIFIER',
  input_label_cols = ['is_fraud_label'],
  auto_class_weights = TRUE,
  max_iterations = 50,
  enable_global_explain = TRUE
) AS
SELECT tipo,
       CAST(monto AS FLOAT64) AS monto,
       CAST(saldo_ant_orig AS FLOAT64) AS saldo_ant_orig,
       CAST(saldo_nuevo_orig AS FLOAT64) AS saldo_nuevo_orig,
       CAST(saldo_ant_dest AS FLOAT64) AS saldo_ant_dest,
       CAST(saldo_nuevo_dest AS FLOAT64) AS saldo_nuevo_dest,
       CAST(err_saldo_orig AS FLOAT64) AS err_saldo_orig,
       CAST(err_saldo_dest AS FLOAT64) AS err_saldo_dest,
       MOD(step, 24) AS hora_dia,
       is_fraud_label
FROM `PROJECT_ID.lumina_silver.transacciones_batch`
WHERE tipo IN ('TRANSFER', 'CASH_OUT')
  AND MOD(ABS(FARM_FINGERPRINT(event_id)), 10) < 8
  AND (is_fraud_label = 1 OR MOD(ABS(FARM_FINGERPRINT(CONCAT('s', event_id))), 10) = 0);

-- Evaluación sobre el 20 % reservado, SIN submuestreo (distribución real ≈ 0.3 % fraude en TRANSFER/CASH_OUT)
CREATE OR REPLACE VIEW `PROJECT_ID.lumina_ml.v_eval_fraude` AS
SELECT tipo,
       CAST(monto AS FLOAT64) AS monto,
       CAST(saldo_ant_orig AS FLOAT64) AS saldo_ant_orig,
       CAST(saldo_nuevo_orig AS FLOAT64) AS saldo_nuevo_orig,
       CAST(saldo_ant_dest AS FLOAT64) AS saldo_ant_dest,
       CAST(saldo_nuevo_dest AS FLOAT64) AS saldo_nuevo_dest,
       CAST(err_saldo_orig AS FLOAT64) AS err_saldo_orig,
       CAST(err_saldo_dest AS FLOAT64) AS err_saldo_dest,
       MOD(step, 24) AS hora_dia,
       is_fraud_label
FROM `PROJECT_ID.lumina_silver.transacciones_batch`
WHERE tipo IN ('TRANSFER', 'CASH_OUT') AND MOD(ABS(FARM_FINGERPRINT(event_id)), 10) >= 8;

-- Consultas de evidencia (ejecutar y capturar):
-- SELECT * FROM ML.EVALUATE(MODEL `PROJECT_ID.lumina_ml.modelo_fraude`, TABLE `PROJECT_ID.lumina_ml.v_eval_fraude`, STRUCT(0.5 AS threshold));
-- SELECT * FROM ML.ROC_CURVE(MODEL `PROJECT_ID.lumina_ml.modelo_fraude`, TABLE `PROJECT_ID.lumina_ml.v_eval_fraude`);
-- SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `PROJECT_ID.lumina_ml.modelo_fraude`);
-- Exportar el modelo XGBoost para servirlo en Dataflow/Cloud Run (fase 2):
-- EXPORT MODEL `PROJECT_ID.lumina_ml.modelo_fraude` OPTIONS (URI = 'gs://PROJECT_ID-lumina-curated/modelos/fraude/');

-- Scoring ML de segunda línea sobre el stream (programar cada 5-15 min como Scheduled Query)
CREATE TABLE IF NOT EXISTS `PROJECT_ID.lumina_ml.scoring_fraude_stream` (
  event_id STRING, event_ts TIMESTAMP, cliente_id STRING, prob_fraude FLOAT64, decision_reglas STRING, scored_ts TIMESTAMP
) PARTITION BY DATE(event_ts);

-- Ventana de 15 min alineada con la frecuencia del Scheduled Query; el NOT EXISTS evita duplicados.
INSERT INTO `PROJECT_ID.lumina_ml.scoring_fraude_stream`
SELECT event_id, event_ts, cliente_id,
       (SELECT p.prob FROM UNNEST(predicted_is_fraud_label_probs) p WHERE p.label = 1) AS prob_fraude,
       decision, CURRENT_TIMESTAMP()
FROM ML.PREDICT(MODEL `PROJECT_ID.lumina_ml.modelo_fraude`, (
  SELECT event_id, event_ts, cliente_id, decision, tipo,
         CAST(monto AS FLOAT64) AS monto,
         CAST(saldo_ant_orig AS FLOAT64) AS saldo_ant_orig,
         CAST(saldo_nuevo_orig AS FLOAT64) AS saldo_nuevo_orig,
         CAST(saldo_ant_dest AS FLOAT64) AS saldo_ant_dest,
         CAST(saldo_nuevo_dest AS FLOAT64) AS saldo_nuevo_dest,
         CAST(saldo_nuevo_orig + monto - saldo_ant_orig AS FLOAT64) AS err_saldo_orig,
         CAST(saldo_ant_dest + monto - saldo_nuevo_dest AS FLOAT64) AS err_saldo_dest,
         MOD(step, 24) AS hora_dia
  FROM `PROJECT_ID.lumina_silver.transacciones_stream` s
  WHERE tipo IN ('TRANSFER', 'CASH_OUT')
    AND event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)
    AND NOT EXISTS (SELECT 1 FROM `PROJECT_ID.lumina_ml.scoring_fraude_stream` x
                    WHERE x.event_id = s.event_id
                      AND x.event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
));

-- ---------------------------------------------------------------- Liquidez (ARIMA_PLUS multiserie)
-- Entrena con los primeros 27 días; los últimos días quedan para evaluar el pronóstico.
CREATE OR REPLACE MODEL `PROJECT_ID.lumina_ml.modelo_liquidez`
OPTIONS (
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'hora',
  time_series_data_col = 'salidas_efectivo',
  time_series_id_col = 'distrito_id',
  data_frequency = 'HOURLY',
  holiday_region = 'PE',
  horizon = 168,
  auto_arima = TRUE
) AS
SELECT hora, distrito_id, CAST(salidas_efectivo AS FLOAT64) AS salidas_efectivo
FROM `PROJECT_ID.lumina_gold.fact_liquidez_hora`
WHERE hora < TIMESTAMP_ADD((SELECT MIN(hora) FROM `PROJECT_ID.lumina_gold.fact_liquidez_hora`), INTERVAL 27 DAY);

-- Evaluación del pronóstico (MAPE diario por distrito) sobre los días reservados
CREATE OR REPLACE TABLE `PROJECT_ID.lumina_ml.eval_liquidez_diaria` AS
WITH pron AS (
  SELECT distrito_id, DATE(forecast_timestamp) AS fecha, SUM(forecast_value) AS pronostico
  FROM ML.FORECAST(MODEL `PROJECT_ID.lumina_ml.modelo_liquidez`, STRUCT(96 AS horizon, 0.9 AS confidence_level))
  GROUP BY 1, 2
),
real AS (
  SELECT distrito_id, DATE(hora) AS fecha, SUM(CAST(salidas_efectivo AS FLOAT64)) AS real_salidas
  FROM `PROJECT_ID.lumina_gold.fact_liquidez_hora` GROUP BY 1, 2
)
SELECT distrito_id, fecha, real_salidas, pronostico,
       SAFE_DIVIDE(ABS(real_salidas - pronostico), real_salidas) AS ape
FROM pron JOIN real USING (distrito_id, fecha);
-- MAPE: SELECT distrito_id, AVG(ape) AS mape FROM `PROJECT_ID.lumina_ml.eval_liquidez_diaria` GROUP BY 1;

-- Plan de liquidez: efectivo requerido por distrito y día = suma horaria en la cota superior (90 %).
-- En el laboratorio el pronóstico arranca al final del periodo de entrenamiento; en producción el DAG
-- diario reentrena con toda la historia y el primer día pronosticado es T+1.
CREATE OR REPLACE TABLE `PROJECT_ID.lumina_ml.pronostico_liquidez` AS
SELECT distrito_id, forecast_timestamp AS hora,
       forecast_value AS salidas_pronosticadas,
       prediction_interval_lower_bound AS cota_inferior,
       prediction_interval_upper_bound AS cota_superior
FROM ML.FORECAST(MODEL `PROJECT_ID.lumina_ml.modelo_liquidez`, STRUCT(168 AS horizon, 0.9 AS confidence_level));

CREATE OR REPLACE VIEW `PROJECT_ID.lumina_gold.v_plan_liquidez_t1` AS
SELECT p.distrito_id, d.nombre AS distrito, DATE(p.hora) AS fecha,
       ROUND(SUM(p.salidas_pronosticadas), 2) AS efectivo_esperado,
       ROUND(SUM(p.cota_superior), 2) AS efectivo_recomendado
FROM `PROJECT_ID.lumina_ml.pronostico_liquidez` p
JOIN `PROJECT_ID.lumina_gold.dim_distrito` d USING (distrito_id)
GROUP BY 1, 2, 3;
