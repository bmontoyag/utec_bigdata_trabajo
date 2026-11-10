"""DAG de precalentamiento de Dataflow (Cloud Composer / Apache Airflow).

Los días de pago (15 y último día del mes) y las vísperas de feriado el volumen sube ×4. Dos horas antes
de la apertura se sube el mínimo de workers del job streaming de 4 a 20, y por la noche se devuelve a 4.
Así el pico no espera al autoscaling (ver el informe de la Entrega 2, sección 4).

Variables de Airflow: lumina_project_id, lumina_region, lumina_dataflow_job (nombre del job).
"""
import calendar
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.sensors.time_delta import TimeDeltaSensor

PROJECT_ID = Variable.get("lumina_project_id")
REGION = Variable.get("lumina_region", default_var="us-central1")
JOB = Variable.get("lumina_dataflow_job", default_var="lumina-fraude-stream")
MIN_NORMAL, MIN_PICO, MAX_WORKERS = 4, 20, 40  # fase MVP, medido en la Entrega 2 (§4.2)


def es_dia_de_pico(ds: str) -> bool:
    dia = datetime.strptime(ds, "%Y-%m-%d").date()
    ultimo = calendar.monthrange(dia.year, dia.month)[1]
    if dia.day in (15, ultimo):
        return True
    manana = (dia + timedelta(days=1)).isoformat()
    filas = BigQueryHook(use_legacy_sql=False, location="US").get_records(
        f"SELECT COUNT(*) FROM `{PROJECT_ID}.lumina_bronze.feriados` WHERE fecha = '{manana}'")
    return filas[0][0] > 0


def ajustar(task_id: str, minimo: int) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"JOB_ID=$(gcloud dataflow jobs list --project {PROJECT_ID} --region {REGION} --status active "
            f"--filter='name={JOB}' --format='value(id)' | head -1) && "
            f"gcloud dataflow jobs update-options $JOB_ID --project {PROJECT_ID} --region {REGION} "
            f"--min-num-workers {minimo} --max-num-workers {MAX_WORKERS}"),
    )


with DAG(
    dag_id="lumina_precalentamiento_dataflow",
    start_date=datetime(2026, 11, 1),
    schedule="0 11 * * *",  # 06:00 hora de Lima: dos horas antes de la apertura de agencias
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["lumina", "streaming", "finops"],
) as dag:
    pico = ShortCircuitOperator(task_id="es_dia_de_pico", python_callable=es_dia_de_pico,
                                op_kwargs={"ds": "{{ data_interval_end | ds }}"})  # día de ejecución
    subir = ajustar("subir_minimo_workers", MIN_PICO)
    # 14 h después de las 06:00 (20:00 Lima); "reschedule" libera el slot del worker mientras espera
    esperar = TimeDeltaSensor(task_id="esperar_fin_del_pico", delta=timedelta(hours=14), mode="reschedule",
                              poke_interval=1800)
    bajar = ajustar("bajar_minimo_workers", MIN_NORMAL)
    pico >> subir >> esperar >> bajar
