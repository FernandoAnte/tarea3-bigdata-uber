from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Inicializar la sesión de Spark para Streaming
spark = SparkSession.builder \
    .appName("UberStreamingConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definir el esquema de los datos entrantes (JSON)
schema = StructType([
    StructField("dispatching_base_num", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# 3. Leer la transmisión de datos en tiempo real desde Kafka (Puerto 9092)
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "movilidad_streaming") \
    .load()

# 4. Parsear el valor binario de Kafka a formato string y aplicar el esquema JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Agrupación por Ventana de Tiempo (1 minuto) y por Base de Despacho
windowed_counts = parsed_df \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("dispatching_base_num")
    ) \
    .agg(count("*").alias("total_solicitudes"))

# 6. Escribir los resultados en la consola en tiempo real (modo update/complete)
query = windowed_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
