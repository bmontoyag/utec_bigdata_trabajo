# utec_bigdata_trabajo — Lumina Bank (DataBank Metropolitan)

Proyecto final del **Taller de Arquitectura Big Data — UTEC, ciclo 2026-2**: plataforma de datos batch + streaming en Google Cloud para la Iniciativa Financiera Lumina.

| Carpeta | Contenido |
|---|---|
| [`doc/`](doc/) | Enunciado del caso, entregables del curso, **Entrega 1** (`Entrega 1 - Arquitectura Lumina Bank en GCP.md`) y **Entrega 2** (`latex/informe_entrega2.pdf`, que atiende la retroalimentación de la E1; arquitectura C4 en `arquitectura/lumina.c4` y evidencias en `evidencias/`) |
| [`presentacion/`](presentacion/) | Slides de sustentación final (`.pptx`, con notas del orador): resumen, situación actual, KPI, productos, supuestos, arquitectura, calculadora, demo, conclusiones y próximos pasos |
| [`src/`](src/) | Implementación: infraestructura, simuladores, Dataflow (streaming), Dataproc (batch), SQL BigQuery/BQML, DAG de Managed Service for Apache Airflow (antes Composer) y API en Cloud Run. La guía paso a paso está en [`src/README.md`](src/README.md) |

## Entrega 3: exposición y demo

| Qué | Dónde |
|---|---|
| Demo end-to-end (simulador → Pub/Sub → Dataflow → BigQuery → API) | [`src/demo/demo.sh`](src/demo/demo.sh); pasos en [`src/README.md`](src/README.md) §7b |
| API de datos (Cloud Run, cuenta `sa-lumina-api`, solo vistas seudonimizadas de `lumina_api`) | [`src/api/`](src/api/), [`src/sql/07_api.sql`](src/sql/07_api.sql), [`src/infra/api_acceso.sh`](src/infra/api_acceso.sh) |
| Tablero (Data Studio sobre BigQuery) | https://datastudio.google.com/reporting/f55e63b1-de65-481d-82d8-ad6ecbdd74bf |
| Calculadora de consumo (7 servicios principales, USD 2,973/mes en us-central1) | https://cloud.google.com/products/calculator/estimate-preview/CiRlNzVlZWEwMi00YjkyLTQwYWUtOGY3ZC0wYWRjY2NjYjQ5MWIQAQ%3D%3D |
| Evidencias nuevas | `doc/evidencias/logs/19d_dataproc_vpc`, `31c_cloud_run_sa_api`, `33b_api_seudonimizada`, `35_demo_latencia`; capturas `36_tablero` y `37_calculadora` en `doc/img/evidencias/` |

**Arquitectura en una línea:** Pub/Sub → Dataflow (Beam) → Memorystore + BigQuery para el fraude en tiempo real, y Cloud Storage → Dataproc (Spark) → BigQuery (medallion + estrella) → BigQuery ML para la liquidez y los informes. Producción corre en `southamerica-west1` con DR en `southamerica-east1`; el laboratorio, en `US`.

> PaySim no se versiona por su tamaño: se descarga de Kaggle (`stealthknight/paysim`).
