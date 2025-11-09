from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, year, month, dayofmonth, hour
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DELTA_PATH = os.getenv("DELTA_PATH", "/data")

print(f"Spark will use Kafka bootstrap: {KAFKA_BOOTSTRAP}")

spark = (
    SparkSession.builder
    .appName("BronzeIngestMachineEvents")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("energy_kw", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("production_count", IntegerType(), True),
])

# Ler do Kafka
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "machine_events")
    # Improve resilience: increase timeouts and backoff to avoid transient disconnects
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.reconnect.backoff.ms", "1000")
    .option("kafka.reconnect.backoff.max.ms", "10000")
    .option("kafka.retry.backoff.ms", "500")
    .option("failOnDataLoss", "false")
    .load()
)

df_parsed = (
    df_raw
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))
)

# 🔹 Adicionar colunas de partição
df_partitioned = (
    df_parsed
    .withColumn("year", year("timestamp"))
    .withColumn("month", month("timestamp"))
    .withColumn("day", dayofmonth("timestamp"))
    .withColumn("hour", hour("timestamp"))
)

# Escrever no Delta particionado
(
    df_partitioned.writeStream
    .format("delta")
    .option("checkpointLocation", f"{DELTA_PATH}/checkpoints/machine_events")
    .outputMode("append")
    .partitionBy("year", "month", "day", "hour")  # ✅ particionamento
    .start(f"{DELTA_PATH}/bronze/machine_events")
    .awaitTermination()
)

query = (
    df_partitioned.writeStream
    .format("delta")
    .option("checkpointLocation", f"{DELTA_PATH}/checkpoints/machine_events")
    .outputMode("append")
    .partitionBy("year", "month", "day", "hour")
    .start(f"{DELTA_PATH}/bronze/machine_events")
)

query.awaitTermination()