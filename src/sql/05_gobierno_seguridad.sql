-- =====================================================================
-- 05_gobierno_seguridad.sql — Acceso por entidad (12 bancos) y enmascaramiento de PII
-- Ejecutar: sed "s/PROJECT_ID/$PROJECT_ID/g" sql/05_gobierno_seguridad.sql | bq query --use_legacy_sql=false --location=US
-- =====================================================================

-- Qué entidad puede ver cada usuario o grupo. En producción la mantiene Seguridad desde el IdP.
CREATE TABLE IF NOT EXISTS `PROJECT_ID.lumina_gold.acceso_entidad` (
  usuario STRING NOT NULL OPTIONS (description = 'correo del usuario (SESSION_USER)'),
  entidad_id INT64 NOT NULL
) OPTIONS (description = 'Gobierno: mapeo usuario → entidad para la vista por entidad');

-- Vista por entidad: cada usuario ve solo las filas de su banco y con la PII tokenizada.
-- Se publica como VISTA AUTORIZADA: los analistas de cada entidad reciben acceso a la vista, no a Silver.
CREATE OR REPLACE VIEW `PROJECT_ID.lumina_gold.v_transacciones_entidad`
OPTIONS (description = 'Seguridad: filas filtradas por entidad del usuario y PII tokenizada') AS
SELECT t.event_id, t.event_ts, t.fecha, t.tipo, t.monto, t.canal, t.entidad_id, t.distrito_id,
       TO_HEX(SHA256(CONCAT('lumina-salt', t.cliente_id))) AS cliente_token,  -- seudónimo estable
       ROUND(t.lat, 2) AS lat_aprox, ROUND(t.lon, 2) AS lon_aprox,            -- geo con menor precisión
       t.score, t.decision, t.origen
FROM `PROJECT_ID.lumina_gold.fact_transacciones` t
JOIN `PROJECT_ID.lumina_gold.acceso_entidad` a
  ON a.entidad_id = t.entidad_id AND a.usuario = SESSION_USER();

-- Señal de fraude compartida entre las 12 entidades, sin datos personales (Vertical I: fraude entre bancos)
CREATE OR REPLACE VIEW `PROJECT_ID.lumina_gold.v_senal_fraude_compartida`
OPTIONS (description = 'Gobierno: destinos sospechosos compartidos entre entidades, sin PII') AS
SELECT TO_HEX(SHA256(CONCAT('lumina-salt', destino_id))) AS destino_token,
       COUNT(DISTINCT entidad_id) AS entidades_afectadas,
       COUNT(*) AS alertas,
       MAX(event_ts) AS ultima_alerta
FROM `PROJECT_ID.lumina_silver.alertas_fraude`
WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY destino_token
HAVING entidades_afectadas >= 2 OR alertas >= 3;

-- Demo: dar acceso a la entidad 3 al usuario que ejecuta (reemplazar por los correos reales)
INSERT INTO `PROJECT_ID.lumina_gold.acceso_entidad` (usuario, entidad_id)
SELECT SESSION_USER(), 3
FROM UNNEST([1])
WHERE NOT EXISTS (SELECT 1 FROM `PROJECT_ID.lumina_gold.acceso_entidad` WHERE usuario = SESSION_USER());

-- Evidencia: debe devolver solo entidad_id = 3
-- SELECT entidad_id, COUNT(*) FROM `PROJECT_ID.lumina_gold.v_transacciones_entidad` GROUP BY 1;

-- Producción: Row Access Policy nativa por grupo de cada entidad (no se crea en el laboratorio porque,
-- una vez creada, quien no esté en ninguna política deja de ver filas, incluida la API y los pipelines).
-- CREATE ROW ACCESS POLICY rap_entidad_03 ON `PROJECT_ID.lumina_silver.transacciones_batch`
--   GRANT TO ('group:analistas-entidad-03@lumina.pe', 'serviceAccount:sa-lumina-pipelines@PROJECT_ID.iam.gserviceaccount.com')
--   FILTER USING (entidad_id = 3);
