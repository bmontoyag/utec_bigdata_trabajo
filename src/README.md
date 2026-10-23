# Lumina Bank — Implementación en GCP (Batch + Streaming)

Código de la plataforma descrita en `doc/Entrega 1 - Arquitectura Lumina Bank en GCP.md`.

```
infra/setup.sh                       APIs, buckets, IAM, Pub/Sub, datasets (ejecuta sql/01_ddl.sql)
simulator/                           Productores de eventos (PaySim → tx-eventos, telemetría → telemetria)
dataflow/fraud_streaming_pipeline.py Streaming: reglas de fraude con estado → BigQuery + alertas
dataproc/batch_paysim_etl.py         Batch: CSV raw → Parquet curado (Spark)
sql/01..04                           DDL, dimensiones, Gold/KPIs, BigQuery ML
orchestration/                       DAG de Cloud Composer (Airflow)
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
  --enable_streaming_engine --max_num_workers 3 --machine_type n1-standard-2 \
  --job_name lumina-fraude-stream
cd ..

# En otra terminal: producir eventos (desde simulator/)
cd simulator
python publish_transactions.py --project $PROJECT_ID --csv ../paysim.csv \
  --solo-transfer-cashout --rate 100 --limit 50000
python publish_telemetry.py --project $PROJECT_ID --terminales 50 --intervalo 5 --minutos 20
```
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
  --set-env-vars PROJECT_ID=$PROJECT_ID --no-allow-unauthenticated
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" <URL>/alertas?minutos=30
```

## 8. Orquestación (Composer)

Copia `orchestration/dag_lumina_batch_diario.py` y la carpeta `sql/` a `dags/` del entorno y define las variables de Airflow
`lumina_project_id` y `lumina_region`. Composer cuesta aprox. USD 0.5/h: créalo solo para la demo.

## 9. Apagar recursos (¡importante!)

```bash
gcloud dataflow jobs drain $(gcloud dataflow jobs list --region $REGION --status=active --format='value(id)') --region $REGION
# Si se crearon: borrar instancia de Memorystore y entorno de Composer
```
