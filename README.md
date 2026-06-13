# Real-Time-E-Commerce-Pipeline
________________________________________________________________________________________________________________________________________________________________________________________________________________

![image](https://github.com/user-attachments/assets/b730e3cd-374a-48ef-b0a0-760bd64035e1)  ![image](https://github.com/user-attachments/assets/a82a462d-be71-49da-bc05-060b36a031a3)  ![image](https://github.com/user-attachments/assets/64553162-699a-42db-949e-46d760b35d23)  ![image](https://github.com/user-attachments/assets/0ebf8c00-ca21-49a0-bcb1-3af27ce15afa)



![image](https://github.com/user-attachments/assets/4747f791-d9ee-4578-a1e8-137926ee151b)


## 🎯 Descripción del Proyecto
La mayoría de los proyectos de Kafka se detienen en imprimir mensajes en una consola. Este proyecto simula un entorno de producción real: un pipeline de comercio electrónico (e-commerce) de extremo a extremo que ingesta eventos en tiempo real, aplica garantías de calidad de datos ***(semántica At-Least-Once)***, y delega la carga analítica a Snowflake.

El proyecto aplica el patrón ***Medallion Architecture*** sobre un flujo de streaming continuo:

**Bronze (Kafka):** Ingesta cruda de eventos caóticos.

**Silver (Python/Kafka):** Limpieza, validación y enriquecimiento en tiempo real.

**Gold (Snowflake):** Ingesta por micro-lotes y transformaciones analíticas vía SQL.


## 🏗️ Arquitectura del Pipeline

[ Python Producer ]  (Genera datos con 25% de errores intencionales y timestamps del pasado)   
▼🌀 [ Kafka Topic: raw_events ]  <-- CAPA BRONZE      
▼[ Stream Processor (Python) ]   | (Validación de reglas de negocio, descarte de basura, Manual Commits)   
▼🌀 [ Kafka Topic: clean_events ] <-- CAPA SILVER (Streaming)     
▼[ Snowflake Loader (Python) ]   | (Agrupa en micro-lotes de 10, usa write_pandas para alta velocidad)   
▼❄️ [ Snowflake Table: KAFKA_EVENTS_SILVER ] <-- ALMACENAMIENTO ANALÍTICO      
▼📈 [ SQL Gold Transformations & Dashboards ]


## 🧠 Decisiones Arquitectónicas

**1. Inyección Controlada de Caos (El Productor)**

El generador de datos no es aleatorio puro. Está diseñado con un propósito arquitectónico:

**25% de Eventos Inválidos:** Simula fallos reales de sensores o apps móviles (IDs nulos, montos negativos, tipos de evento no reconocidos).
**Backdated Timestamps:** Genera eventos con fechas de hasta 6 días en el pasado. Esto es vital para preparar el terreno para probar estrategias de Watermarks y manejo de Late Data en sistemas como Spark Streaming.

**2. Preservación del Orden por Key (Particionamiento)**
Al usar customer_id como Message Key en el productor, se garantiza que todos los eventos de un mismo cliente se enruten a la misma partición de Kafka. Esto asegura que, si un cliente añade un producto al carrito y luego lo compra en el siguiente segundo, el consumidor final leerá esos eventos en el orden exacto en que ocurrieron.

**3. Semántica "At-Least-Once" y Gestión de Offsets**
Tanto el stream_processor.py como el snowflake_consumer.py utilizan enable_auto_commit=False.

**El problema del Auto-Commit:** Si Kafka avanzara el offset automáticamente al leer un mensaje, y el script falla justo antes de escribir en Snowflake, esos datos se perderían para siempre.
**La solución:** El offset solo se confirma (consumer.commit()) después de una inserción exitosa en Snowflake o en el tópico limpio. Cero pérdida de datos.

**4. Micro-batching para Snowflake (El Consumidor Final)**
Hacer una conexión a Snowflake por cada evento individual destruiría la red y los créditos de la cuenta cloud. El snowflake_consumer.py implementa un patrón de búfer: acumula 10 eventos en memoria (BATCH_SIZE = 10), crea un DataFrame de Pandas y utiliza la función write_pandas de Snowflake para hacer una inserción masiva (Bulk Insert) altamente eficiente.

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()
![image]()
![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()
