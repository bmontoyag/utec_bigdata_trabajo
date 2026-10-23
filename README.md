# utec_bigdata_trabajo — Lumina Bank (DataBank Metropolitan)

Proyecto final del **Taller de Arquitectura Big Data — UTEC, ciclo 2026-2**: plataforma de datos batch + streaming en Google Cloud para la Iniciativa Financiera Lumina.

| Carpeta | Contenido |
|---|---|
| [`doc/`](doc/) | Enunciado del caso, entregables del curso y **documento de la Entrega 1** (`Entrega 1 - Arquitectura Lumina Bank en GCP.md`) |
| [`presentacion/`](presentacion/) | Slides de sustentación para el comité ejecutivo (`.pptx`, con notas del orador) |
| [`src/`](src/) | Implementación: infraestructura, simuladores, Dataflow (streaming), Dataproc (batch), SQL BigQuery/BQML, DAG de Composer y API en Cloud Run. La guía paso a paso está en [`src/README.md`](src/README.md) |

**Arquitectura en una línea:** Pub/Sub → Dataflow (Beam) → Memorystore + BigQuery para el fraude en tiempo real, y Cloud Storage → Dataproc (Spark) → BigQuery (medallion + estrella) → BigQuery ML para la liquidez y los informes. Producción corre en `southamerica-west1` con DR en `southamerica-east1`; el laboratorio, en `US`.

> PaySim no se versiona por su tamaño: se descarga de Kaggle (`stealthknight/paysim`).
