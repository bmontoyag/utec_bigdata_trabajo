# Lumina Bank — Implementación en GCP (Batch + Streaming)

Código de la plataforma descrita en `doc/Entrega 1 - Arquitectura Lumina Bank en GCP.md`.

```
infra/setup.sh                       APIs, buckets, IAM, Pub/Sub, datasets (ejecuta sql/01_ddl.sql)
infra/gobierno_seguridad_red.sh      VPC privada, KMS (CMEK), auditoría, policy tags, Knowledge Catalog DQ, vistas por entidad
simulator/                           Productores de eventos (PaySim → tx-eventos, telemetría → telemetria)
dataflow/fraud_streaming_pipeline.py Streaming: reglas de fraude con estado → BigQuery + alertas
dataproc/batch_paysim_etl.py         Batch: CSV raw → Parquet curado (Spark)
sql/01..06                           DDL, dimensiones, Gold/KPIs, BigQuery ML, gobierno, validación de capacidad
orchestration/                       DAGs de Managed Service for Apache Airflow (antes Cloud Composer): batch diario y precalentamiento de Dataflow
api/                                 API en Cloud Run (FastAPI)
```

> Todos los comandos se ejecutan desde `src/`. Los archivos SQL usan el marcador `PROJECT_ID`, que se reemplaza con `sed`.

## 0. Prerrequisitos

```bash
export PROJECT_ID=<tu-proyecto>      # proyecto con facturación habilitada
export REGION=us-central1
gcloud auth login && gcloud auth application-default login
python3 -m venv .venv && source .venv/bin/activate
pip install -r simulator/requirements.txt -r dataflow/requirements.txt
```
Crea un **presupuesto con alertas** (Billing → Budgets) antes de empezar.

## 1. Infraestructura base

```bash
bash infra/setup.sh
```

## 2. Subir PaySim al Data Lake (Bronze)

Descarga el CSV desde Kaggle (`stealthknight/paysim`) y verifica que la cabecera sea
`step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud`.

```bash
gcloud storage cp paysim.csv gs://$PROJECT_ID-lumina-raw/paysim/paysim.csv
```

## 3. Dimensiones conformadas

```bash
sed "s/PROJECT_ID/$PROJECT_ID/g" sql/02_dimensiones.sql | bq query --use_legacy_sql=false --location=US
```

## 4. Camino BATCH: Dataproc Serverless (Spark) → BigQuery Silver

```bash
gcloud storage cp dataproc/batch_paysim_etl.py gs://$PROJECT_ID-lumina-temp/code/
gcloud dataproc batches submit pyspark gs://$PROJECT_ID-lumina-temp/code/batch_paysim_etl.py \
  --region $REGION --version 2.2 \
  --service-account sa-lumina-pipelines@$PROJECT_ID.iam.gserviceaccount.com \
  -- --entrada gs://$PROJECT_ID-lumina-raw/paysim/ --salida gs://$PROJECT_ID-lumina-curated/transacciones/

bq load --location=US --replace --source_format=PARQUET \
  --time_partitioning_field=event_ts --time_partitioning_type=DAY \
  --clustering_fields=distrito_id,tipo,cliente_id \
  --hive_partitioning_mode=AUTO \
  --hive_partitioning_source_uri_prefix=gs://$PROJECT_ID-lumina-curated/transacciones/ \
  lumina_silver.transacciones_batch "gs://$PROJECT_ID-lumina-curated/transacciones/*"
```
> Si Dataproc pide una subred con *Private Google Access*:
> `gcloud compute networks subnets update default --region $REGION --enable-private-ip-google-access`

## 5. Camino STREAMING: Pub/Sub → Dataflow → BigQuery

```bash
cd dataflow
python fraud_streaming_pipeline.py \
  --project $PROJECT_ID --region $REGION --runner DataflowRunner \
  --temp_location gs://$PROJECT_ID-lumina-temp/dataflow \
  --service_account_email sa-lumina-pipelines@$PROJECT_ID.iam.gserviceaccount.com \
  --enable_streaming_engine --machine_type n2-standard-2 \
  --num_workers 1 --max_num_workers 2 --autoscaling_algorithm THROUGHPUT_BASED \
  --job_name lumina-fraude-stream
# Si ya corrió infra/gobierno_seguridad_red.sh, agregue (workers sin IP pública, dentro de la VPC):
#   --no_use_public_ips --subnetwork regions/$REGION/subnetworks/lumina-datos-$REGION
cd ..

# En otra terminal: producir eventos (desde simulator/)
cd simulator
python publish_transactions.py --project $PROJECT_ID --csv ../paysim.csv \
  --solo-transfer-cashout --rate 100 --limit 50000
python publish_telemetry.py --project $PROJECT_ID --terminales 50 --intervalo 5 --minutos 20
```
> **Dimensionamiento:** el laboratorio usa 1–2 workers. La prueba de carga midió ~75 ev/s por worker `n2-standard-2`,
> así que la fase MVP usa `--min_num_workers 4 --max_num_workers 40` y el DAG `dag_lumina_precalentamiento.py` sube
> el mínimo a 20 en días de pago (informe de la Entrega 2, sección 4).

Para **Memorystore** (opcional): crea una instancia Redis en la misma VPC y agrega
`--redis_host <IP> --requirements_file requirements-workers.txt --network default` al pipeline.

## 6. Gold + BigQuery ML

```bash
sed "s/PROJECT_ID/$PROJECT_ID/g" sql/03_gold.sql | bq query --use_legacy_sql=false --location=US
sed "s/PROJECT_ID/$PROJECT_ID/g" sql/04_bqml.sql | bq query --use_legacy_sql=false --location=US
```
Evidencias: ejecuta las consultas comentadas de `04_bqml.sql` (ML.EVALUATE, ROC, GLOBAL_EXPLAIN, MAPE) y
`SELECT * FROM lumina_gold.v_desempeno_reglas_fraude`.

## 7. API (Entrega 3)

```bash
gcloud run deploy lumina-api --source api --region $REGION \
  --service-account sa-lumina-pipelines@$PROJECT_ID.iam.gserviceaccount.com \
  --set-env-vars PROJECT_ID=$PROJECT_ID --no-allow-unauthenticated \
  --cpu 1 --memory 512Mi --concurrency 20 --min-instances 0 --max-instances 3 --timeout 60
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" <URL>/alertas?minutos=30
```

Dimensionamiento (doc/Entrega 2, §4): ~20 req/s en el pico × 1.5 s por consulta a BigQuery = 30 solicitudes
simultáneas → 2 instancias con concurrencia 20; el máximo de 3 deja margen y limita las consultas paralelas a
BigQuery (3 × 20 = 60). En producción `--min-instances 1`. El servicio de autorización (producción) usa
`--concurrency 40 --min-instances 2 --max-instances 20 --no-cpu-throttling`.

## 8. Orquestación (Managed Service for Apache Airflow, antes Composer)

Copia `orchestration/*.py` y la carpeta `sql/` a `dags/` del entorno y define las variables de Airflow
`lumina_project_id`, `lumina_region` y `lumina_dataflow_job`. Composer cuesta aprox. USD 0.5/h: créalo solo para la demo.

## 9. Gobierno, seguridad y redes

```bash
bash infra/gobierno_seguridad_red.sh
```
Crea la VPC `lumina-vpc` con subred privada (Private Google Access), firewall interno, llave CMEK con rotación de 90 días
(bucket archive y dataset Gold), sink de auditoría a `lumina_auditoria`, taxonomía de policy tags, escaneo de calidad de
Knowledge Catalog (antes Dataplex) y las vistas `v_transacciones_entidad` y `v_senal_fraude_compartida` (`sql/05_gobierno_seguridad.sql`).

Linaje: habilite `datalineage.googleapis.com` y lance el batch de Spark con
`--properties spark.dataproc.lineage.enabled=true`. Para encadenar la carga a Silver, el SQL de Gold y el streaming
en el mismo grafo, ejecute `python3 infra/registrar_linaje.py` (eventos OpenLineage).

## 10. Validar el dimensionamiento de Dataflow (prueba de carga)

1. Relance el job fijo en **1 worker** (`--num_workers 1 --max_num_workers 1`, `--job_name lumina-carga`).
2. Publique por escalones de 5 minutos: 100 → 300 → 600 → 900 ev/s (sobre 600 ev/s abra 2 terminales en paralelo):
   ```bash
   python publish_transactions.py --project $PROJECT_ID --csv ../paysim.csv --rate 300 --limit 90000
   ```
3. En cada escalón capture en Dataflow: *Throughput (elementos/s)*, *Data freshness*, *System latency* y *CPU*;
   y en Pub/Sub: *Oldest unacked message age* de `tx-eventos-dataflow`.
4. Ejecute `sql/06_validacion_capacidad.sql`: da el throughput por minuto, la latencia p50/p95/p99, la capacidad sostenida
   con p95 < 5 s y el `max_num_workers` recomendado para 5,000 TPS. Anote los resultados en la tabla del §4.4 del documento.

## 11. Apagar recursos (¡importante!)

```bash
gcloud dataflow jobs drain $(gcloud dataflow jobs list --region $REGION --status=active --format='value(id)') --region $REGION
# Si se crearon: borrar instancia de Memorystore y entorno de Composer
```
