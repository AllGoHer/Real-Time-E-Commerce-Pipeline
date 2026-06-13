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
