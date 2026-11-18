# Demo final Lumina Bank: guion de grabación y libreto

Video sin locución, de unos 7 minutos. Muestra el camino streaming funcionando de punta a punta, en vivo sobre GCP: una
transacción sale del simulador, pasa por Pub/Sub y Dataflow, queda decidida en BigQuery y se consulta por la API y el
tablero. En la sustentación el equipo lo narra con este libreto mientras corre.

---

## 1. Antes de grabar (20 minutos antes)

1. Inicia sesión en Chrome con **bmontoyag.pe.gcp@gmail.com**, la cuenta del proyecto `lumina-bank-utec`.
2. Desde `src/`, lanza el job y espera a que el worker esté listo (3 a 5 minutos):
   ```bash
   bash demo/demo.sh iniciar
   bash demo/demo.sh estado      # repetir hasta ver "WORKER LISTO: puede publicar"
   ```
3. Abre estas pestañas en este orden. Si la consola abre otra cuenta, cámbiala con el avatar.

   | Pestaña | URL |
   |---|---|
   | A · Diapositiva de arquitectura streaming | `presentacion/Lumina Bank - Sustentacion.pptx`, diapositiva 8 |
   | B · Data Lake (zona raw) | https://console.cloud.google.com/storage/browser/lumina-bank-utec-lumina-raw?project=lumina-bank-utec |
   | C · Dataproc (Spark) | https://console.cloud.google.com/dataproc/batches?project=lumina-bank-utec |
   | D · BigQuery | https://console.cloud.google.com/bigquery?project=lumina-bank-utec |
   | E · Pub/Sub, tópico de transacciones | https://console.cloud.google.com/cloudpubsub/topic/detail/tx-eventos?project=lumina-bank-utec |
   | F · Dataflow, job de demo | https://console.cloud.google.com/dataflow/jobs?project=lumina-bank-utec (abrir `lumina-demo-…`, pestaña *Job graph*) |
   | G · Cloud Run, API | https://console.cloud.google.com/run/detail/us-central1/lumina-api/metrics?project=lumina-bank-utec |
   | H · Tablero | https://datastudio.google.com/reporting/f55e63b1-de65-481d-82d8-ad6ecbdd74bf |
   | Terminal | En `src/`, con letra grande (Cmd +) |

4. En BigQuery (pestaña D), abre `src/sql/08_demo.sql` en un editor nuevo. Cada escena ejecuta una consulta de ese archivo.
5. Graba la pantalla con QuickTime (Archivo → Nueva grabación de pantalla) o con OBS, a 1080p y sin audio.

## 2. Guion escena por escena

Los tiempos son del video ya editado. Si una espera es larga en la grabación (el worker, la carga de una consulta), córtala
al editar. La columna **Libreto** es lo que dice en vivo quien presenta esa escena.

| # | Tiempo | En pantalla | Acción | Libreto |
|---|---|---|---|---|
| 1 | 0:00–0:30 | A · Diapositiva 8 | Ninguna; señalar las cajas de izquierda a derecha | "Vamos a seguir una transacción de punta a punta. Entra por Pub/Sub, Dataflow la evalúa con reglas que tienen estado, la decisión queda en BigQuery y la exponemos por una API y un tablero. Todo lo que van a ver corre ahora en nuestro proyecto de Google Cloud." |
| 2 | 0:30–1:15 | B → C → D | B: abrir `paysim/` (469 MiB). C: mostrar el batch `lumina-etl-20261118-vpc` en *Succeeded*, con la subred `lumina-datos-us-central1`. D: ejecutar la consulta de la **escena 2** | "Primero, el histórico. PaySim llega a la zona raw del Data Lake; Spark en Dataproc Serverless lo limpia dentro de nuestra VPC privada y lo deja en Parquet. En BigQuery son 6,36 millones de transacciones de julio, ya en el modelo estrella. Sobre esa base corre el streaming." |
| 3 | 1:15–1:45 | F · Job graph | Mostrar el grafo y el panel derecho: 1 worker, región, *Streaming Engine* | "Este es el pipeline de fraude en Dataflow. Lee de Pub/Sub, parsea, manda lo inválido a una cola de errores, y tiene dos etapas con estado: viaje imposible por cliente y velocidad por cuenta destino. Corre en un worker sin IP pública, dentro de la VPC." |
| 4 | 1:45–2:30 | Terminal + E | Terminal: `bash demo/demo.sh publicar`. Cambiar a E · pestaña *Metrics*: sube la tasa de publicación | "Arrancamos el simulador: publica transacciones reales de PaySim a 20 por segundo, con ubicación y dispositivo. Pub/Sub las recibe y las retiene 7 días; si Dataflow cae, no se pierde nada." |
| 5 | 2:30–3:45 | F → D | F: en el grafo, los contadores de elementos suben en cada etapa. D: consultas de decisiones y de latencia de la **escena 5**; re-ejecutar la primera después de 30 s | "Dataflow ya está decidiendo. En BigQuery vemos cada transacción con su decisión: aprobar, revisar o bloquear. Si la ejecutamos de nuevo, los números crecen: es en vivo. La latencia p95, desde que la transacción sale hasta que hay decisión, está por debajo de un segundo; la meta es 5." |
| 6 | 3:45–4:30 | D | Consulta de alertas de la **escena 6**; señalar la columna `reglas` | "Estas son las alertas. Cada una dice qué reglas la dispararon: vaciado de cuenta, destino inconsistente, monto alto, viaje imposible, cuenta mula. El score suma sus pesos; desde 0,8 se bloquea y desde 0,5 va a revisión." |
| 7 | 4:30–5:30 | Terminal → G | Terminal: `bash demo/demo.sh verificar` (al final llama a la API). G: el gráfico de solicitudes muestra el pico | "La misma información sale por la API en Cloud Run. Sin token responde 403. Con token devuelve las alertas, y fíjense: el cliente y el destino salen como seudónimos. La API corre con su propia cuenta de servicio, que solo puede leer vistas autorizadas: nunca toca Silver ni ve un id en claro." |
| 8 | 5:30–6:30 | H → D | H: *Refresh data* (menú ⋮); la tabla de alertas sube. D: consulta del plan de liquidez de la **escena 8** | "Para el negocio, el tablero en Data Studio lee de BigQuery: KPI por origen, transacciones por entidad y alertas, que acaban de subir con la demo. Abajo, el plan de efectivo del día siguiente por distrito, que pronostica BigQuery ML con ARIMA_PLUS." |
| 9 | 6:30–7:00 | Terminal | `bash demo/demo.sh apagar` | "Al terminar drenamos el job: termina de procesar lo que tiene en vuelo y se apaga. Pagamos solo el tiempo que corrió. En producción el mismo job escala de 4 a 55 workers con el DAG de precalentamiento." |

## 3. Después de grabar

- Revisa con `bash demo/demo.sh estado` que no quede ningún job activo.
- Edita el video: corta las esperas y deja cada escena en su tramo de tiempo. Si quieres, agrega como rótulo el número y el
  nombre de la escena; ayuda a seguir el libreto en vivo.
- Lleva el video en local (no dependas de internet) y súbelo también a Drive como respaldo.

## 4. Si algo falla durante la grabación

| Síntoma | Qué hacer |
|---|---|
| `estado` no dice "WORKER LISTO" después de 8 minutos | `bash demo/demo.sh apagar` y vuelve a `iniciar` |
| La consulta de decisiones sale vacía | El worker todavía no procesa: espera 1 minuto y re-ejecuta |
| La latencia de los primeros minutos sale en decenas de segundos | Se publicó antes de que el worker estuviera listo: repite `publicar` y graba el segundo tramo |
| La API responde 403 con token | El token expiró: vuelve a ejecutar `verificar` |
