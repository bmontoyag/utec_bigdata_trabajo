UTEC - Sección 1 

# **Entregables del Proyecto Final: Taller de Arquitectura Big Data** 

El proyecto de fin de materia consiste en el diseño, sustentación e implementación de una plataforma analítica y de datos integral para el proyecto o caso de estudio seleccionado. La evaluación del curso se divide en **3 entregas progresivas** : 

## **<mark>ENTREGA 1: Examen (E - 25%) + Proyecto Parte 1 (P1 - 25%) — Total: 50%</mark>** 

#### **> Fecha de Entrega: 24 Octubre 2026 - hasta 11:59 pm** 

En este primer hito se evalúa la capacidad analítica, el diagnóstico del negocio y la definición del diseño conceptual e infraestructura lógica de la arquitectura de datos. 

### **1. Examen (E): Caso de Estudio y Sustentación (25%)** 

#### **Documento de Sustentación Técnica, Económica y de Resiliencia:** 

- **Justificación de Stack tecnológico:** Sustento de la selección tecnológica priorizando componentes _GCP_ aprendidas en clase y otras que deseen utilizar. 

- **Análisis de Escalabilidad y Performance:** Explicación de cómo la arquitectura soportará aumentos masivos en el volumen de datos y peticiones durante eventos de alta demanda (ej. Cyber Monday, Black Friday o picos de tráfico). 

- **Justificación Económica y Optimización de Costos:** Demostración de cómo la arquitectura optimiza el costo operativo por transacción o pedido procesado. 

- **Plan de Recuperación y Alta Disponibilidad (DRP/HA):** Estrategia de tolerancia a fallos y recuperación ante desastres en caso de caídas de servicios o nodos críticos. 

### **2. Proyecto Parte 1 (P1): Blueprint y Diseño de Ingesta / Almacenamiento (25%)** 

#### **Diseño de Arquitectura End-to-End y Capa de Ingesta/Storage:** 

- **Integración Multifuente:** Estrategia clara para conectar, modelar y cruzar datos heterogéneos (streaming como clics/IoT/sensores y batch como transacciones e inventarios históricos). 

- **Diagrama de Arquitectura End-to-End:** Gráfico explicativo con las capas de Ingesta, Cómputo, Almacenamiento y Exposición. 

UTEC - Sección 1 

- **Definición de Ingesta y Storage:** Selección de mecanismos de captura (streaming/batch) y definición de motores de almacenamiento (Data Lake, Lakehouse, Data Warehouse o NoSQL) adaptados a la velocidad y volumen del negocio 

## **<mark>ENTREGA 2: Proyecto Parte 2 (P2) — Total: 25%</mark>** 

**> Fecha de Entrega: 7 Noviembre 2026 - hasta 11:59 pm -** _Mid-term de Código / Avance de Pipelines_ 

En esta segunda entrega se evalúa el desarrollo práctico de la capa de procesamiento y transformación de datos. 

### **3. Proyecto Parte 2 (P2): Procesamiento, Transformación y Modelado (25%)** 

**Construcción de Pipelines y Lógica de Negocio:** 

- **Pipelines de Cómputo / Transformación:** Código funcional o scripts en motores de procesamiento (Spark, Flink, SQL, etc.) para la limpieza, enriquecimiento y cruce de los datos heterogéneos. 

- **Procesamiento Híbrido:** Implementación funcional de transformaciones para flujos en tiempo real (baja latencia) y procesamientos masivos por lotes (batch). 

- **Modelado para Analítica:** Creación de las estructuras finales de datos (tablas consolidadas, esquemas en estrella/copo de nieve o colecciones optimizadas) listos para su consumo. 

UTEC - Sección 1 

## **<mark>ENTREGA 3: Proyecto Parte 3 (P3) — Total: 25%</mark>** 

- **Fecha de Entrega del Repositorio:** Viernes, 20 de Noviembre de 2026 _(Hasta las 10:00 PM)_ 

- **Sustentación y Demo Final:** Sábado, 21 de Noviembre de 2026, en clase 

En este hito final se evalúa la conexión con aplicaciones consumidoras, la demostración funcional end-to-end y la sustentación del proyecto. 

### **4. Proyecto Parte 3 (P3): Exposición de Datos, Demo End-to-End y Sustentación (25%)** 

**Prototipo Funcional Final y Exposición:** 

- **Capa de Exposición y Servicios (APIs):** Implementación de una consulta, endpoint o API sencilla que exponga los datos procesados hacia las aplicaciones consumidoras, tableros de control o modelos analíticos. 

- **Demostración de Caso de Uso Funcional:** Demostración en vivo de un pipeline corriendo de extremo a extremo sobre al menos un proceso clave del negocio (ej. decisiones en tiempo real, planes de inventario, informes OLAP, etc.). 

- **Defensa y Repositorio Final:** Sustentación final del equipo mostrando la factibilidad técnica del proyecto y entrega del repositorio de código estructurado en github. 

Recordar que esta ultima parte se tendrá nota de participación individual y de trabajo grupal 

Ademas para sustentar tendras que construir un documento en formato Slides con los siguientes bullets: 

1. Resumen ejecutivo - Alcance de la Migración/Implementación 

2. Situación actual 

1. Criterios de éxito, performance, en formato KPI 

2. Productos tecnológicos a usar 

3. Dependencias/supuestos o fuera de scope 

4. Arquitectura final: (la arquitectura que generará la estimación de consumo del primer año) 

5. Calculadora de consumo 

6. Demo end to end de implementación 

7. Conclusiones 

8. Next Steps 

