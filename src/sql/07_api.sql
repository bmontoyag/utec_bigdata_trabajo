-- =====================================================================
-- 07_api.sql — Capa de exposición: vistas que consume la API de Cloud Run
-- La cuenta sa-lumina-api solo lee este dataset; nunca Silver ni Gold directamente.
-- Los identificadores de cliente y destino salen como seudónimo (mismo hash que v_transacciones_entidad).
-- Ejecutar: sed "s/PROJECT_ID/$PROJECT_ID/g" sql/07_api.sql | bq query --use_legacy_sql=false --location=US
-- Luego: bash infra/api_acceso.sh (autoriza las vistas y crea la cuenta de servicio)
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.lumina_api`
OPTIONS (location = 'US', description = 'Exposición: vistas autorizadas para la API de datos');

CREATE OR REPLACE VIEW `PROJECT_ID.lumina_api.v_alertas`
OPTIONS (description = 'Alertas de fraude con cliente y destino seudonimizados') AS
SELECT event_id, event_ts,
       TO_HEX(SHA256(CONCAT('lumina-salt', cliente_id))) AS cliente_token,
       TO_HEX(SHA256(CONCAT('lumina-salt', destino_id))) AS destino_token,
       distrito_id, entidad_id, tipo, monto, score, decision, reglas
FROM `PROJECT_ID.lumina_silver.alertas_fraude`;

CREATE OR REPLACE VIEW `PROJECT_ID.lumina_api.v_transacciones_cliente`
OPTIONS (description = 'Transacciones streaming por seudónimo de cliente, sin ubicación ni dispositivo') AS
SELECT TO_HEX(SHA256(CONCAT('lumina-salt', cliente_id))) AS cliente_token,
       event_ts, tipo, monto, canal, score, decision, reglas
FROM `PROJECT_ID.lumina_silver.transacciones_stream`
WHERE NOT IFNULL(sintetico, FALSE);

CREATE OR REPLACE VIEW `PROJECT_ID.lumina_api.v_perfil_cliente`
OPTIONS (description = 'Perfil del cliente sin identificador ni domicilio') AS
SELECT TO_HEX(SHA256(CONCAT('lumina-salt', c.cliente_id))) AS cliente_token,
       c.entidad_id, c.distrito_id, d.nombre AS distrito, c.segmento_etario, c.preferencia_canal
FROM `PROJECT_ID.lumina_gold.dim_cliente` c
JOIN `PROJECT_ID.lumina_gold.dim_distrito` d USING (distrito_id);

CREATE OR REPLACE VIEW `PROJECT_ID.lumina_api.v_plan_liquidez_t1`
OPTIONS (description = 'Plan de efectivo T+1 por distrito') AS
SELECT * FROM `PROJECT_ID.lumina_gold.v_plan_liquidez_t1`;

CREATE OR REPLACE VIEW `PROJECT_ID.lumina_api.v_kpi_operativos_diario`
OPTIONS (description = 'KPI operativos diarios por entidad y origen') AS
SELECT * FROM `PROJECT_ID.lumina_gold.v_kpi_operativos_diario`;
