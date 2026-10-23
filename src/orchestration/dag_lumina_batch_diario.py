"""DAG diario de Lumina Bank (Cloud Composer / Apache Airflow).

raw (GCS) → Dataproc Serverless (Spark) → curated (Parquet) → BigQuery Silver → Gold → BQML (liquidez)

Despliegue: copiar este archivo y la carpeta sql/ a la carpeta dags/ del entorno Composer y definir
las variables de Airflow `lumina_project_id` y `lumina_region`.
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

PROJECT_ID = Variable.get("lumina_project_id")
REGION = Variable.get("lumina_region", default_var="us-central1")
SA = f"sa-lumina-pipelines@{PROJECT_ID}.iam.gserviceaccount.com"
CURATED = f"gs://{PROJECT_ID}-lumina-curated/transacciones/"
SQL_DIR = Path(__file__).parent / "sql"


def sql(nombre: str) -> str:
    return (SQL_DIR / nombre).read_text().replace("PROJECT_ID", PROJECT_ID)


def job_sql(task_id: str, archivo: str) -> BigQueryInsertJobOperator:
    return BigQueryInsertJobOperator(
        task_id=task_id,
        location="US",
        configuration={"query": {"query": sql(archivo), "useLegacySql": False}},
    )


with DAG(
    dag_id="lumina_batch_diario",
    start_date=datetime(2026, 11, 1),
    schedule="0 2 * * *",  # 02:00 UTC: el plan de liquidez queda listo antes de las 05:00
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10), "sla": timedelta(hours=2)},
    tags=["lumina", "batch"],
) as dag:

    etl_spark = DataprocCreateBatchOperator(
        task_id="etl_paysim_spark",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="lumina-etl-{{ ds_nodash }}-{{ ti.try_number }}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{PROJECT_ID}-lumina-temp/code/batch_paysim_etl.py",
                "args": ["--entrada", f"gs://{PROJECT_ID}-lumina-raw/paysim/", "--salida", CURATED],
            },
            "runtime_config": {"version": "2.2"},
            "environment_config": {"execution_config": {"service_account": SA}},
        },
    )

    cargar_silver = BigQueryInsertJobOperator(
        task_id="cargar_silver",
        location="US",
        configuration={"load": {
            "sourceUris": [f"{CURATED}*"],
            "sourceFormat": "PARQUET",
            "destinationTable": {"projectId": PROJECT_ID, "datasetId": "lumina_silver",
                                 "tableId": "transacciones_batch"},
            "writeDisposition": "WRITE_TRUNCATE",  # idempotente
            "timePartitioning": {"type": "DAY", "field": "event_ts"},
            "clustering": {"fields": ["distrito_id", "tipo", "cliente_id"]},
            "hivePartitioningOptions": {"mode": "AUTO", "sourceUriPrefix": CURATED},
        }},
    )

    dimensiones = job_sql("dimensiones_gold", "02_dimensiones.sql")
    gold = job_sql("hechos_gold", "03_gold.sql")
    modelos = job_sql("modelos_bqml", "04_bqml.sql")

    etl_spark >> cargar_silver >> dimensiones >> gold >> modelos
