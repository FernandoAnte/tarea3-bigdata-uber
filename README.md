# Descripción de la Solución
Este proyecto implementa una arquitectura de procesamiento de datos utilizando componentes del ecosistema Big Data:
* **Apache Kafka:** Como plataforma de mensajería y transmisión de eventos en tiempo real.
* **Apache Spark Streaming:** Para el consumo, parseo y procesamiento de datos en memoria mediante ventanas temporales.
* **Entorno:** Máquina Virtual con VirtualBox, PySpark, HDFS y Kafka (puerto `9092`).

Instrucciones de Ejecución
Sigue los siguientes pasos en tu entorno de máquina virtual (VirtualBox / PuTTY) para poner en marcha la arquitectura de streaming:

1. Iniciar Zookeeper
Abre una terminal e inicia el servicio de Zookeeper:
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties

2. Iniciar el Servidor de Kafka
En una segunda terminal, inicia el servidor de Kafka asegurando que escuche en el puerto 9092:
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties

3. Crear el Topic de Streaming
Crea el topic donde se transmitirán los eventos de movilidad:
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic movilidad_streaming

4. Ejecutar el Productor de Uber
En una tercera terminal, ejecuta el script productor para comenzar a enviar los datos de tráfico simulados:
python3 uber_producer.py

5. Ejecutar el Consumidor de Spark Streaming
Finalmente, en una cuarta terminal, lanza la aplicación de Spark Streaming con el conector de Kafka para procesar las solicitudes agrupadas por base y ventanas de 1 minuto:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 uber_streaming_consumer.py


Indicadores Principales
Streaming (En tiempo real): Conteo dinámico de solicitudes agrupadas por número de base de despacho (dispatching_base_num) en intervalos de tiempo deslizantes de 1 minuto para la detección oportuna de picos de demanda.
