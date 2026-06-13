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

* **25% de Eventos Inválidos:** Simula fallos reales de sensores o apps móviles (IDs nulos, montos negativos, tipos de evento no reconocidos).
* **Backdated Timestamps:** Genera eventos con fechas de hasta 6 días en el pasado. Esto es vital para preparar el terreno para probar estrategias de Watermarks y manejo de Late Data en sistemas como Spark Streaming.

**2. Preservación del Orden por Key (Particionamiento)**
Al usar customer_id como Message Key en el productor, se garantiza que todos los eventos de un mismo cliente se enruten a la misma partición de Kafka. Esto asegura que, si un cliente añade un producto al carrito y luego lo compra en el siguiente segundo, el consumidor final leerá esos eventos en el orden exacto en que ocurrieron.

**3. Semántica "At-Least-Once" y Gestión de Offsets**
Tanto el stream_processor.py como el snowflake_consumer.py utilizan enable_auto_commit=False.

**El problema del Auto-Commit:** Si Kafka avanzara el offset automáticamente al leer un mensaje, y el script falla justo antes de escribir en Snowflake, esos datos se perderían para siempre.
**La solución:** El offset solo se confirma (consumer.commit()) después de una inserción exitosa en Snowflake o en el tópico limpio. Cero pérdida de datos.

**4. Micro-batching para Snowflake (El Consumidor Final)**
Hacer una conexión a Snowflake por cada evento individual destruiría la red y los créditos de la cuenta cloud. El snowflake_consumer.py implementa un patrón de búfer: acumula 10 eventos en memoria (BATCH_SIZE = 10), crea un DataFrame de Pandas y utiliza la función write_pandas de Snowflake para hacer una inserción masiva (Bulk Insert) altamente eficiente.


## 🛠️ Tech Stack

**Streaming:** Apache Kafka (Desplegado en modo KRaft sin ZooKeeper vía Docker).

**Procesamiento:** Python Puro (kafka-python).

**Data Warehouse:** Snowflake (Ingesta vía Connector y Pandas Tools).

**Serialización:** JSON (Simulando contratos de datos desacoplados).

## 📂 Estructura del Código

Estructura del Código: 

                      ├── docker-compose.yml          # Infraestructura KRaft de Kafka
                      ├── producer.py                 # Generador de eventos (Bronze Ingestion)
                      ├── stream_processor.py         # Motor de validación y limpieza (Silver Processing)
                      ├── snowflake_consumer.py       # Ingesta por micro-lotes a Snowflake (Data Loader)
                      └── requirements.txt            # Dependencias del proyecto

**Desglose de Archivos:**

***producer.py:*** Genera el flujo continuo. Maneja la serialización JSON y asigna la Key de partición.

***stream_processor.py:*** Aplica reglas de negocio estrictas (Catálogo de eventos válidos, montos positivos). Filtra el ruido antes de que contamine el Data Warehouse.

***snowflake_consumer.py:*** Actúa como puente entre el mundo del Streaming (Kafka) y el mundo Batch (Snowflake). Gestiona la conversión de tipos de datos de Python a Snowflake (mayúsculas) y la inyección masiva. 


## ⚙️ Cómo ejecutar este proyecto

**Infraestructura:** Levantar el clúster de Kafka mediante <mark>**docker-compose up -d.**</mark>

**Tópicos:** Crear los tópicos necesarios (<mark>**raw_events, clean_events**</mark>) con 3 particiones.

**Configuración Snowflake:** Actualizar las credenciales en <mark>**snowflake_consumer.py**</mark> y crear la tabla destino <mark>**KAFKA_EVENTS_SILVER.**</mark>

**Ejecución en orden:**

**Terminal 1:** <mark>python producer.py</mark> (Iniciar la lluvia de eventos).

**Terminal 2:** <mark>python stream_processor.py</mark> (Limpiar en tiempo real).

**Terminal 3:** <mark>python snowflake_consumer.py</mark> (Cargar a la nube).


## 🚀 DESARROLLO

1.	Iniciamos activando el docker desktop.
   
2.	Creamos una carpeta Kafka-ecom en la terminal.

   Código:

           mkdir kafka-ecom

luego ingresamos a la carpeta creada

  Código:

          cd kafka-ecom

C:\Users\User\kafka-ecom>

![image](https://github.com/user-attachments/assets/a17e73ee-d1cd-4946-8827-f01c3627f209)

3.	Luego abrimos VSC y vinculamos con la carpeta Kafka-ecom y, luego creamos nuestro archivo docker-compose.yaml para levantar Kafka en modo KRaft (sin ZooKeeper), que es la arquitectura de última generación.

Código:

        services:
          kafka:
            image: apache/kafka:latest
            container_name: kafka
            ports:
              - "9092:9092"
              - "29092:29092"
            environment:
              # ---- KRaft core ----
              KAFKA_NODE_ID: 1
              KAFKA_PROCESS_ROLES: broker,controller
              KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093

              # ---- Listeners (ALL roles must appear here) ----
              KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_EXTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093

              # ---- Advertised addresses ----
              KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_EXTERNAL://localhost:29092

              # ---- Controller configuration ----
              KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
              KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

              KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
              KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
              KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1

              # ---- Protocol mapping ----
              KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT

              # ---- Storage ----
              KAFKA_LOG_DIRS: /tmp/kraft-combined-logs

          kafka-ui:
            image: provectuslabs/kafka-ui:latest
            container_name: kafka-ui
            depends_on:
              - kafka
            ports:
              - "8080:8080"
            environment:
              KAFKA_CLUSTERS_0_NAME: local-kafka
              KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092


🧠<mark>**EXPLICACION DEL CÓDIGO**</mark>

Ahora desglosare y explicare paso a paso en que consiste este código de docker-compose.

**•	Sección 1:** El Servicio Principal (kafka)

Código: 

        kafka:

Define el nombre del primer contenedor que se va a levantar.

Código: 

         image: apache/kafka:latest

Le dice a Docker que descargue la imagen oficial directamente de Apache (no de terceros como Confluent). La versión latest incluye el modo KRaft nativo.

Código:

         container_name: Kafka

Le da un nombre fijo al contenedor. Esto es vital para la red interna de Docker. Permite que otros contenedores (como la UI) lo encuentren escribiendo "kafka" en lugar de una IP aleatoria.

Código:

        ports:
          - "9092:9092"
          - "29092:29092"

Mapeo de puertos (Host : Contenedor):

•	9092: Se usa principalmente para la comunicación interna entre contenedores de Docker.
•	29092: Es el puerto que expones a tu computadora Windows (Host). Cuando tu código Python usa localhost:29092, entra por aquí.

Código:

        environment:

Aquí comienzan las variables de configuración interna del servidor de Kafka.

Código:

         # ---- KRaft core ----
         KAFKA_NODE_ID: 1
   
En la era antigua (con ZooKeeper), esto se llamaba broker.id. Es el identificador único de este servidor dentro del clúster.

Código:

          KAFKA_PROCESS_ROLES: broker,controller


¡LA LÍNEA MÁS IMPORTANTE DE ESTE ARCHIVO! En el pasado, necesitabas levantar 3 servidores de ZooKeeper aparte para gestionar los metadatos (qué tópico existe, dónde están las particiones). Al poner broker,controller, le estás diciendo a este contenedor: "Tú vas a ser el que reciba los datos (broker) Y el que guarde la metadata (controller)". Esto elimina la necesidad de ZooKeeper por completo.

Código:

         KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093

Como este nodo es el controlador, necesita saber cómo votar consigo mismo. Le dice: "El votante número 1 está en la dirección de red interna kafka en el puerto 9093".

Código:

         # ---- Listeners (ALL roles must appear here) ----
         KAFKA_LISTENERS:           PLAINTEXT://0.0.0.0:9092,PLAINTEXT_EXTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093

Los "Oídos" de Kafka. Le dice en qué direcciones físicas debe abrir puertos para escuchar conexiones.

•	0.0.0.0 significa "escucha en todas las redes disponibles".

•	Define 3 oídos: uno para dentro de Docker (PLAINTEXT), uno para tu Windows (PLAINTEXT_EXTERNAL), y uno privado para la comunicación del controlador (CONTROLLER en el 9093).

Código:

          # ---- Advertised addresses ----
          KAFKA_ADVERTISED_LISTENERS:      PLAINTEXT://kafka:9092,PLAINTEXT_EXTERNAL://localhost:29092

La "Tarjeta de Presentación" de Kafka. Cuando un cliente (Python o la UI) se conecta, Kafka le devuelve esta lista para decirle: "Para hablarme, usa estas direcciones".

•	Si te conectas desde otro contenedor Docker, Kafka le devuelve kafka:9092.

•	Si te conectas desde tu Windows, Kafka te devuelve localhost:29092. 

Código:

        # ---- Controller configuration ----
        KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

Enlaza el nombre lógico "CONTROLLER" con el socket físico del puerto 9093 definido en los Listeners.

Código:

        KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

Si tuvieras varios servidores de Kafka, ellos hablarían entre sí usando el protocolo PLAINTEXT (puerto 9092).

Código:
     
        KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
        KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
        KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
        
La regla de la supervivencia (Replicación). Estas variables le dicen a Kafka: "Copia los datos de control interno (offsets y transacciones) a '1' servidor". ¿Por qué 1? Porque solo tienes UN contenedor de Kafka levantado. Si tuvieras 3 contenedores de Kafka, esto tendría que ser 3 para que los datos sobrevivan si un servidor se quema.

Código:

         # ---- Protocol mapping ----
         KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:          PLAINTEXT:PLAINTEXT,PLAINTEXT_EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT

Mapea los nombres de los listeners a protocolos de seguridad. En este caso, todo es PLAINTEXT (sin encriptación SSL/SASL), que es lo correcto para un entorno de aprendizaje local.

Código:

       # ---- Storage ----
       KAFKA_LOG_DIRS: /tmp/kraft-combined-logs

El directorio físico dentro del contenedor Linux donde se guardarán los mensajes JSON que envía tu productor.py en el disco duro.

**Sección 2: La Interfaz Gráfica (kafka-ui)**

Código:

        kafka-ui:
        image: provectuslabs/kafka-ui:latest
        container_name: kafka-ui


Instancia una herramienta visual muy popular para no tener que usar la terminal negra de Linux para ver los mensajes.

Código:

        depends_on:
         - kafka

Orden de arranque vital. Le dice a Docker: "No enciendas la UI hasta que el contenedor kafka esté 100% levantado". Si no pones esto, la UI intentará conectarse a Kafka antes de que exista y fallará.

Código:
    
        ports:
          - "8080:8080"

Expone la interfaz web en tu navegador Windows: http://localhost:8080.

Código:

        environment:
          KAFKA_CLUSTERS_0_NAME: local-kafka

El nombre bonito que verás en el menú desplegable al entrar a la página web.

Código:
      
        KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092

¡OJO AQUÍ! Fíjate que NO dice localhost:29092. ¿Por qué? Porque la UI es un contenedor Docker. Si la UI usara localhost, estaría buscando a Kafka dentro de su propio contenedor, y ahí no hay nada. Al usar kafka:9092, usa la red interna de Docker para hablar directamente con el otro contenedor. ¡Es la misma red que usan los consumidores de Python si los correras dentro de Docker!


Ahora volvemos al desarrollo del proyecto. Levantaremos el docker-compose para crear el container.

Código:

         docker compose up -d


![image](https://github.com/user-attachments/assets/4d925b50-d273-4e82-a664-4f49d6737a70)

Ahora verificamos si ya está activo el contenedor en docker desktop.

![image](https://github.com/user-attachments/assets/a7b0dd08-9d85-4160-8a0a-4c1cc0b02766)

Ahora abrimos la interface de usuario (localhost:8080)

![image](https://github.com/user-attachments/assets/6b7fed07-c034-460b-9b29-9d5016a51cb2)

Ahora crearemos un entorno virtual para nuestro proyecto.

Código:

          python -m venv venv

podemos verificar la creación del entorno virtual en VSC.


![image](https://github.com/user-attachments/assets/c22e1991-ac7d-4ce3-af26-06ed84367d99)

Ahora activamos el entorno creado.

código:

         venv\Scripts\activate


![image](https://github.com/user-attachments/assets/f24e0e45-32b8-4737-88e9-7db175f4447e)

Código:

          pip freeze

código:

          pip install kafka-python


![image](https://github.com/user-attachments/assets/ad1a6879-d9ac-4638-8ad7-dc6011f87c2d)


Nuevamente aplicamos pip freeze para ver la nueva dependencia instalada

Código:

          pip freeze


![image](https://github.com/user-attachments/assets/7dda8e2e-cacd-47da-b102-7fd2a11735e5)


•	Crearemos la carpeta de requerimientos desde la terminal.

Código:

          pip freeze > requirements.txt


![image](https://github.com/user-attachments/assets/6065867c-d952-4633-928d-817667c40b7d)

•	Ahora creamos el archivo producer.py en VSC. 

Este archivo producer.py no es un simple generador de datos aleatorios. Es una herramienta de pruebas de estrés y calidad (Test Harness) diseñada específicamente para poner a prueba pipelines de Streaming.

Está programado para simular el caos del mundo real: datos que llegan tarde, datos que están rotos, y diferentes tipos de usuarios.

Código:

        import json
        import random
        import time
        import uuid
        from datetime import datetime, timedelta, timezone
        from kafka import KafkaProducer

        BOOTSTRAP_SERVERS="localhost:29092"
        Topic_NAME= "raw_events"

        Producer = KafkaProducer(
            bootstrap_servers="localhost:29092",  #BOOTSTRAP_SERVERS,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
    
        )

        EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]
        INVALID_EVENT_TYPES = ["CLICK", "VIEW", "PAY"]

        def random_timestamp_last_6_days():
            now = datetime.now(timezone.utc)
            past = now - timedelta(days=6)

            random_seconds = random.uniform(0, (now - past).total_seconds())
            return past + timedelta(seconds=random_seconds)

        def generate_event():
            is_invalid = random.random() < 0.25

            customer_id = f"CUST_{random.randint(1,5)}"
            event_type = random.choice(EVENT_TYPES)
            amount = round(random.uniform(10,500),2)
            currency = "USD"

            invalid_field = None
            if is_invalid:
                invalid_field = random.choice([
                    "customer_id",
                    "event_type",
                    "amount",
                    "currency"
                ])
    
            event = {
                "event_id": str(uuid.uuid4()),
                "customer_id": None if invalid_field == "customer_id" else customer_id,
                "event_type": (
                    random.choice(INVALID_EVENT_TYPES)
                    if invalid_field == "event_type"
                    else event_type
                ),
                "amount": (
                    random.uniform(-500, -10)
                    if invalid_field == "amount"
                    else amount
                ),
                "currency": None if invalid_field == "currency" else currency,
                "event_timestamp": random_timestamp_last_6_days().isoformat(),
                "is_valid": not is_invalid,
                "invalid_field": invalid_field
            }

            return event["customer_id"], event

        print("Starting Kafka producer...")

        while True:
            key, event = generate_event()

            Producer.send(
                topic=Topic_NAME,
                key=key,
                value=event
            )

            print(f"Produced event | key={key} | valid={event['is_valid']}")

            time.sleep(1)


<mark>**Explicación del Código**</mark>

A continuación explicare lo que hace este código línea por línea.

**1. Importaciones y Configuración Inicial**

Código:

        import json
        import random
        import time
        import uuid
        from datetime import datetime, timedelta, timezone
        from kafka import KafkaProducer

•	json: Para convertir los diccionarios de Python en cadenas de texto JSON.

•	random / time: Para generar aleatoriedad y pausar la ejecución.

•	uuid: Para generar identificadores universalmente únicos (evita colisiones de IDs).

•	timezone: Importación moderna para manejar tiempos UTC sin que Python lance advertencias.

•	KafkaProducer: El cliente que se conecta a Kafka.


Código:

        BOOTSTRAP_SERVERS="localhost:29092"
        Topic_NAME= "raw_events"

•	Definimos el servidor de conexión (tu Docker en Windows) y el nombre del canal (tópico) donde se van a arrojar los eventos.

Código:

        Producer = KafkaProducer(
            bootstrap_servers="localhost:29092",  # Conexión a Kafka
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

•	Nota: Aquí se esta usando la variable escrita directamente en lugar de la variable BOOTSTRAP_SERVERS. Funciona igual, pero es una buena práctica usar la variable para no repetir código.

•	key_serializer: Kafka exige que las Keys sean bytes. Esta función anónima (lambda) dice: "Si me envías una Key (ej. 'CUST_1'), conviértela a bytes UTF-8. Si me envías un None, déjalo como None".

•	value_serializer: Kafka exige que los Values sean bytes. Esta lambda hace un proceso de 2 pasos: json.dumps(v) convierte el diccionario en un string JSON, y .encode("utf-8") lo convierte en bytes.

  Código:
  
          EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]
          INVALID_EVENT_TYPES = ["CLICK", "VIEW", "PAY"]
          
•	Definimos qué eventos son "correctos" para nuestro negocio y cuáles son basura que no debería procesar el sistema de analítica.

**2. La Función de Tiempo (Simulando "Late Data")**

Código:

        def random_timestamp_last_6_days():
            now = datetime.now(timezone.utc)
            past = now - timedelta(days=6)

            random_seconds = random.uniform(0, (now - past).total_seconds())
            return past + timedelta(seconds=random_seconds)

•	now: Obtiene la fecha y hora exacta de este segundo en UTC.

•	past: Resta 6 días a la fecha actual.

•	random_seconds: Calcula cuántos segundos hay en esos 6 días (518,400 segundos) y elige un número al azar entre 0 y ese máximo.

•	return: Toma la fecha de hace 6 días y le suma esos segundos aleatorios.

•	💡 Para qué sirve: Esto genera eventos que parecen haber ocurrido hace 1 día, 3 días o 5 días. Es fundamental para probar los Watermarks (Marcas de agua) en Databricks, para ver si tu sistema es capaz de procesar un evento viejo que llega tarde.

**3. La Fábrica de Eventos (Simulando Datos Sucios)**

Código:

        def generate_event():
        is_invalid = random.random() < 0.25
        
•	Lanza un dado virtual. Hay un 25% de probabilidad de que el evento que se va a generar sea defectuoso (simula fallos de red, bugs en la app móvil, sensores rotos).

Código:

        customer_id = f"CUST_{random.randint(1,5)}"
        event_type = random.choice(EVENT_TYPES)
        amount = round(random.uniform(10,500),2)
        currency = "USD"

•	Crea datos "buenos" por defecto. Solo hay 5 clientes ficticios (del 1 al 5) para que sea fácil rastrearlos. Montos aleatorios entre 10 y 500.

Código:
    
        invalid_field = None
        if is_invalid:
           invalid_field = random.choice([
                "customer_id",
                "event_type",
                "amount",
                "currency"
            ])
            
•	Si el evento fue marcado como inválido, decide qué campo se va a corromper de forma aleatoria.

Código:

        event = {
            "event_id": str(uuid.uuid4()), # ID único irrepetible (ej: '550e8400-e29b...')
            "customer_id": None if invalid_field == "customer_id" else customer_id,

•	Si el campo a corromper era el ID del cliente, lo pone como Nulo (None).

Código:

        "event_type": (
            random.choice(INVALID_EVENT_TYPES)
            if invalid_field == "event_type"
            else event_type
        ),
        
•	Si el campo a corromper era el tipo de evento, cambia un "PURCHASE" válido por un "CLICK" inválido (que no existe en nuestro catálogo válido).

Código:

        "amount": (
            random.uniform(-500, -10)
            if invalid_field == "amount"
            else amount
        ),
        
•	Si el campo a corromper era el monto, genera un número negativo (lo cual es un error crítico en finanzas).

Código:

        "currency": None if invalid_field == "currency" else currency,
        "event_timestamp": random_timestamp_last_6_days().isoformat(),

•	Si la moneda era el campo roto, la pone en Nulo. El timestamp siempre usa la función del pasado y lo convierte a formato texto estándar ISO (ej: "2024-05-20T14:32:10.123456+00:00").

Código:

          "is_valid": not is_invalid,
          "invalid_field": invalid_field
       }

•	Bandera de calidad: Pone True o False para que los sistemas downstream puedan filtrar rápido. invalid_field dice exactamente por qué falló (esto es oro puro para hacer dashboards de "Calidad de Datos").

Código:

        return event["customer_id"], evento

•	Devuelve una tupla. El customer_id lo separa porque lo usará como Key de Kafka. El resto del diccionario es el Value.

**4.	El Bucle Infinito (El Motor de Streaming)**

Código:

        print("Starting Kafka producer...")

        while True:

•	Bucle infinito. El productor no se detiene hasta que presiones Ctrl + C. Simula una aplicación móvil enviando datos eternamente.

Código:

         key, event = generate_event()

•	Llama a la fábrica y desempaqueta la tupla.

Código:

        Producer.send(
            topic=Topic_NAME,
            key=key,
            value=event
        )

•	topic: A qué canal mandarlo (raw_events).

•	key=key: Al enviar el CUST_1 como key, Kafka garantiza que todos los eventos de ese cliente caigan en la misma partición. Esto asegura que, si el cliente compró y luego devolvió, los eventos se leen en el orden exacto en que ocurrieron.

•	value=event: El diccionario JSON completo.

Código:

        print(f"Produced event | key={key} | valid={event['is_valid']}")

        time.sleep(1)

•	Imprime en tu consola para que veas que está trabajando.

•	time.sleep(1): MUY IMPORTANTE. Pausa la ejecución 1 segundo. Si quitas esto, Python inundará tu red y tu CPU intentando enviar millones de eventos por segundo, y tu computadora colapsará. Simula un ritmo realista de llegada de datos.


•	Luego crearemos el tema.

Código:

        docker exec -it kafka bash


![image](https://github.com/user-attachments/assets/3e4c2c77-be75-41e4-a690-13e5caad753a)

Código:

	       cd /opt/kafka/bin

•	Ahora creamos un tópico y definimos la arquitectura de escalabilidad y resistencia a fallos.

código:

         ./kafka-topics.sh --create --topic raw_events --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1

<mark>**NOTA: ¿Qué es lo que quiere decir este código?**</mark>

*Desglose línea por línea:*

•	**./kafka-topics.sh** Es el script ejecutable que viene con la instalación de Kafka. Está en la carpeta /opt/kafka/bin/. El ./ le dice a la consola de Linux: "Ejecuta este archivo que está en la carpeta actual".

•	**--create** Es la acción. Le dice a Kafka que no queremos leer, listar o borrar, sino crear un nuevo tópico desde cero.

•	**--topic raw_events** Es el nombre que le damos al "canal" donde vivirán los datos. 

💡 Nota de Data Engineer: El nombre raw_events no es casualidad. Sugiere que estamos implementando la capa Bronze de la arquitectura Medallion en Kafka. Está guardando los eventos crudos (raw) tal como llegan, sin limpiar.

•	**--bootstrap-server kafka:9092** Es la dirección IP y el puerto del clúster de Kafka al que te vas a conectar. 💡 Nota del entorno: Aquí está la magia del archivo docker-compose.yml. Si estuviéramos en Windows, tendría que usar localhost:29092. Pero como ejecutamos este comando dentro del contenedor Docker (en la terminal negra de Linux), usa el nombre interno del contenedor (kafka) y el puerto interno (9092). Es la ruta más rápida y directa.

•	**--partitions 3** (⚠️ Esta es la parte más importante) Le estás diciendo a Kafka: "No guardes todos los mensajes en una sola fila gigante. Divide este tópico en 3 cajas separadas (particiones)". ¿Por qué 3? Por escalabilidad y paralelismo. Si tienes 3 consumidores en tu group_id, Kafka automáticamente asignará la Partición 0 al Consumidor 1, la 1 al Consumidor 2, y la 2 al Consumidor 3. Pueden leer los datos al mismo tiempo. Si tuvieras 1 sola partición, los 3 consumidores se pelearían por ella y 2 se quedarían ociosos.

•	**--replication-factor 1** Le estás diciendo a Kafka: "No hagas copias de seguridad de este tópico". ¿Por qué 1? Esto está directamente atado a tu docker-compose.yml. En el archivo se puso KAFKA_NODE_ID: 1. Solo se tiene un servidor (Broker) de Kafka corriendo. Si pusiera --replication-factor 2, Kafka intentaría copiar los datos a un segundo servidor... pero como no existe, el comando fallaría con un error de Insufficient replicas. En producción real (ej. AWS, Confluent), esto sería 3, por lo que, si un servidor se quema, los datos siguen seguros en los otros dos.


Ahora verificamos en la interface de usuario de Kafka.

![image](https://github.com/user-attachments/assets/dceea1b9-3b58-4c74-b2e7-c2a550915d93)

![image](https://github.com/user-attachments/assets/1293dfdf-8d7f-495c-9c5a-3aa346b7257d)

![image](https://github.com/user-attachments/assets/fb468831-dd65-4b00-86f5-28f890207d5a)

Ahora abrimos una segunda terminal y creamos el mismo entorno virtual.

![image](https://github.com/user-attachments/assets/13da95f8-0e07-49cf-b14f-1fb489921c37)

Y ejecutamos producer.py

![image](https://github.com/user-attachments/assets/5d19f77c-bd29-416a-b463-3c1ec2ddda82)

•	Luego de un momento, rompemos el bucle con control C.

Y verificamos en Kafka UI (localhost:8080)

![image](https://github.com/user-attachments/assets/9171d139-9f14-41a5-b059-6364f5a00692)

•	Ahora creamos un Motor de Procesamiento de Streaming en Tiempo Real que implementa exactamente la transición de la capa Bronze a la capa Silver de la Arquitectura Medallion, pero en Kafka. 

Regresamos a VSC y creamos un archivo llamado stream_processor.py.

Código:

        import json
        from kafka import KafkaConsumer, KafkaProducer

        BOOTSTRAP_SERVERS="localhost:29092"
        INPUT_TOPIC="raw_events"
        OUTPUT_TOPIC="clean_events"
        GROUP_ID="silver-stream-processor"

        VALID_EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]

        consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers="localhost:29092",
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )

        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        def is_valid_event(event):
            if not event.get("customer_id"):
                return False
            if event.get("event_type") not in VALID_EVENT_TYPES:
                return False
            if event.get("amount") is None or event.get("amount") <= 0:
                return False
            if not event.get("currency"):
                return False
            if event.get("is_valid") is not True:
                return False
            return True

        print("Starting Silver Stream Processor....")

        for message in consumer:
            key = message.key
            event = message.value

            if is_valid_event(event):
                producer.send(
                    topic=OUTPUT_TOPIC,
                    key=key,
                   value=event
                )
                print(f"FORWARDED | key={key} | event_type={event['event_type']}")
            else:
                print(f"DROPPED | key={key} | reason=invalid")
    
            consumer.commit()

#### 🧠 EXPLICACIÓN DEL CÓDIGO

Ahora pasare a explicar línea por línea en que consiste el presente código.

**1.	Importaciones y Configuración Arquitectónica**

Código:

        import json
        from kafka import KafkaConsumer, KafkaProducer

•	Importamos las herramientas necesarias. Usamos un solo script para hacer dos cosas: consumir (leer) y producir (escribir).

Código:

        BOOTSTRAP_SERVERS="localhost:29092"
        INPUT_TOPIC="raw_events"
        OUTPUT_TOPIC="clean_events"
        GROUP_ID="silver-stream-processor"

•	Arquitectura Medallion en Kafka: Aquí defines el flujo de tus datos. Entran por raw_events (Capa Bronze) y salen por clean_events (Capa Silver).
•	GROUP_ID: Este es el "DNI" de este script ante los ojos de Kafka. Si este programa se apaga, Kafka recordará que "silver-stream-processor" leyó hasta cierto punto, y cuando lo enciendas de nuevo, continuará exactamente desde ahí.

Código:

        VALID_EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]

•	Diccionario de datos: Aquí definimos la "verdad" de nuestro negocio. Cualquier cosa que no esté en esta lista se considerará basura.

**2.	El Consumidor (Lectura del caos)**

Código:

        consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers="localhost:29092",
            group_id=GROUP_ID,
            auto_offset_reset="earliest",

•	Nos conectamos al tópico sucio. earliest significa que, si es la primera vez que corremos esto, leerá todos los mensajes históricos acumulados.

Código:

         enable_auto_commit=False,

•	🔥 ¡LA LÍNEA MÁS IMPORTANTE DE SEGURIDAD DE ESTE CÓDIGO!
•	Por defecto, Kafka guarda tu progreso en cuanto lee el mensaje. Si lo apagas aquí, perderías datos. Al ponerlo en False, le dices a Kafka: "No anotes mi progreso hasta que yo te diga explícitamente que terminé de procesarlo". Esto garantiza la semántica At-Least-Once (Cero pérdidas de datos).

Código:

           key_deserializer=lambda k: k.decode("utf-8") if k else None,
           value_deserializer=lambda v: json.loads(v.decode("utf-8"))
       )

•	Deserialización Inversa: El productor convirtió el diccionario a Bytes. Esto hace el proceso inverso: Bytes → String → Diccionario de Python. 

Ahora puedes escribir event["amount"] como si fuera un objeto normal de Python.

**3.	El Productor (Preparación para lo limpio)**

Código:

        producer = KafkaProducer(
            bootstrap_servers="localhost:29092",
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

•	Es la misma configuración del primer productor. Se prepara para inyectar los datos ya limpios al nuevo tópico. Convirtiendo diccionarios a Bytes otra vez.

**4.	La Lógica de Negocio (El filtro de calidad)**

Código:

        def is_valid_event(event):
            if not event.get("customer_id"):
                return False

•	El truco  .get(): En lugar de hacer event["customer_id"] (que haría explotar el código si el campo no existe con un KeyError), usamos .get(). Si el campo no viene, devuelve None (que en Python es falso), y rechaza el evento.

Código:

       if event.get("event_type") not in VALID_EVENT_TYPES:
           return False

•	Filtro de Catálogo: Si llega un "CLICK" o "VIEW" (los datos sucios que inventó tu productor), lo bloquea.

Código:

       if event.get("amount") is None or event.get("amount") <= 0:
           return False

•	Regla de Negocio Financiero: Un monto nulo o negativo (que el productor generaba aleatoriamente) es inválido. Se descarta. Un cliente no puede pagar cantidades negativas.

Código:

        if not event.get("currency"):
            return False
        if event.get("is_valid") is not True:
            return False
        return True

•	Verificaciones finales de moneda y una redundancia de seguridad usando la bandera que venía en el JSON original.

**5.	El Bucle Infinito (El Motor en marcha)**

Código:

        print("Starting Silver Stream Processor....")

        for message in consumer:

•	El bucle se queda "vivo" eternamente, esperando que lleguen mensajes del tópico raw_events.

Código:

        key = message.key
        event = message.value

•	Extrae la llave (ej. CUST_1) y el diccionario JSON que ya Python convirtió en un objeto.

Código:

        if is_valid_event(event):
            producer.send(
                topic=OUTPUT_TOPIC,
                key=key,
                value=event
            )
            print(f"FORWARDED | key={key} | event_type={event['event_type']}")

•	El happy path: Si pasó todas las reglas de negocio, lo inyecta en el tópico clean_events.

•	Detalle vital: ¡Estamos pasando la misma key! Esto asegura que el orden del cliente se mantenga intacto en la capa Silver.

Código:

        else:
            print(f"DROPPED | key={key} | reason=invalid")

•	El camino de la basura: Si no pasó la validación, simplemente lo ignora e imprime que lo tiró. 

Código:
              
        consumer.commit()

•	La línea que cierra el ciclo de seguridad. Como pusimos enable_auto_commit=False arriba, el avance de lectura solo se guarda si el código llega hasta esta línea final. Si el script se mata a mitad del if/else, el commit no pasa, y al reiniciar, Kafka volverá a mandar el último mensaje. Cero pérdidas de datos.

El resultado visual en tu consola

Cuando ejecutes esto (junto con el producer.py en otra ventana), verás algo así, demostrando que tu filtro de calidad funciona:

text:

      FORWARDED | key=CUST_2 | event_type=PAGE_VIEW
      DROPPED   | key=CUST_1 | reason=invalid  <-- (Era un monto negativo o un CLICK)
      FORWARDED | key=CUST_3 | event_type=PURCHASE
      DROPPED   | key=CUST_5 | reason=invalid  <-- (Faltaba el customer_id)


•	Ahora en la terminal corremos el archivo stream_processor.py.

código:

         python stream_processor.py


![image](https://github.com/user-attachments/assets/1c3e9a15-ce3b-4ed3-a16b-4069b366c4a9)

Luego vamos a la interface de usuario de Kafka.

Aquí se puede observar los desplazamientos

![image](https://github.com/user-attachments/assets/c08361fc-c996-43b8-98ec-1c9a7c6af6b7)

Y ahora observamos en pestaña de consumer.


![image](https://github.com/user-attachments/assets/4f201689-ce24-4ba9-ade4-32a6b9a3502b)


_______________________________________________________________________________________________________________________________________________________________________________________________________________
### CAPA DE ORO CON SNOWFLAKE
_______________________________________________________________________________________________________________________________________________________________________________________________________________

1.	Creamos una base datos.

![image](https://github.com/user-attachments/assets/7af89d7a-2427-408d-a409-d65a08cd290d)

2.	Creamos el esquema.

![image](https://github.com/user-attachments/assets/a10c2244-00d1-4bf8-9574-310f14bf44ff)

3.	Creamos una tabla.

![image](https://github.com/user-attachments/assets/6a68cce8-240e-45c6-a4c2-6aa10f032973)

![image](https://github.com/user-attachments/assets/6f16a0f0-faff-40cd-815d-a37973982ada)

4.	Ahora vamos a VSC y creamos un archivo llamado snowflake_consumer.py.

Código:

        import json
        from kafka import KafkaConsumer
        import snowflake.connector
        from snowflake.connector.pandas_tools import write_pandas
        import pandas as pd

        BOOTSTRAP_SERVERS="localhost:29092"
        TOPIC_NAME="clean_events"
        GROUP_ID = "snowflake-loader"

        SNOWFLAKE_CONFIG = {
            "user" : "<<USERNAME>>",
            "password" : "<<PASSWORD>>",
            "account": "<<ACCOUNT>>",
            "warehouse" : "COMPUTE_WH",
            "database" : "KAFKA_DB",
            "schema" : "STREAMING"
        }

        BATCH_SIZE = 10

       consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers= “localhost:29092”,
            group_id=GROUP_ID,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )

        sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)

        print("Connected to Snowflake")
        print("Starting Kafka -> Snowflake Loader...")

        buffer = []

        def flush_to_snowflake(records):
            df = pd.DataFrame(records)
            df.columns = [c.upper() for c in df.columns]

            success, nchunks, nrows, _ = write_pandas(
                conn=sf_conn,
                df=df,
                table_name="KAFKA_EVENTS_SILVER"
            )

            if not success:
                raise Exception("Snowflake insert failed")
    
            print(f"Inserted {nrows} rows into Snowflake")

        for message in consumer:
            event = message.value
            buffer.append({
                "event_id": event["event_id"],
                "customer_id" : event["customer_id"],
                "event_type" : event["event_type"],
                "amount" : event["amount"],
                "currency" : event["currency"],
                "event_timestamp" : event["event_timestamp"]
            })

            if len(buffer) >= BATCH_SIZE:
                try:
                    flush_to_snowflake(buffer)
                    consumer.commit()
                    buffer.clear()
                except EXCEPTION as e:
                    print(f"ERROR inserting batch: {e}")


5.	Ahora en la terminal, antes de ejecutar el código anterior, vamos a conectar snowflake con nuestro código de python y también instalar pandas para convertir los datos en un marco de datos(DataFrame) y, pyarrow.

Código:

        Pip install snowflake-connector-python pandas pyarrow

![image](https://github.com/user-attachments/assets/8900148b-0a18-4853-b174-b33a75090b38)

Código:

         Python snowflake_consumer.py


![image](https://github.com/user-attachments/assets/81bb3284-a1e4-4d25-9ae1-98f5210183f1)

![image](https://github.com/user-attachments/assets/440b6ddd-e7ce-4e9e-ad89-a012ce78b152)

Ahora verificamos la conexión en snowflake.

![image](https://github.com/user-attachments/assets/216ba69a-bc97-4e1d-ab16-80dbc356d1ec)

Luego en un rato lo interrumpimos con control C

Ahora procesamos los datos de snowflake y crearemos algunas tablas.

![image](https://github.com/user-attachments/assets/b0b4eeef-19af-498b-9cd3-380db42a1cc0)

![image](https://github.com/user-attachments/assets/a01558f6-f7c6-460c-be91-9b08765f4ffc)

Luego verificamos si se han creado las vistas.

![image](https://github.com/user-attachments/assets/18d713a6-615e-4e8d-90c9-663ea53d3816)

![image](https://github.com/user-attachments/assets/2033f9a0-3b9d-41ca-b45a-bc6b83a027af)

•	Ahora crearemos un panel de control.

![image](https://github.com/user-attachments/assets/a9fd1d51-4ff6-4ace-9309-a4c4f041369a)

Luego seleccionamos la base de datos con la que vamos trabajar.

![image](https://github.com/user-attachments/assets/690d288c-546d-48d9-bdd3-f6367e93bdb1)

![image](https://github.com/user-attachments/assets/e272db7a-52da-43a3-926a-459dc9ebbd1f)

![image](https://github.com/user-attachments/assets/5c8da13c-acbd-4736-ada8-0cc064310d4f)

![image](https://github.com/user-attachments/assets/ea0df002-a2fa-4d21-9763-cf4969924cb3)

![image](https://github.com/user-attachments/assets/467e2935-e6b6-4916-8d3d-8a29aae3b743)

![image](https://github.com/user-attachments/assets/bf583e47-4274-4c2e-9627-cb1961adc1ea)

![image](https://github.com/user-attachments/assets/56c2e6c1-cd6a-490e-a961-41bcfd04a52a)
