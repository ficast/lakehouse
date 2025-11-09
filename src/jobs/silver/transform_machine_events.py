import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, when,
    year, month, dayofmonth, hour, lit
)
from delta.tables import DeltaTable

# === Configurações ===
DELTA_PATH = os.getenv("DELTA_PATH", "/data")
BRONZE_PATH = f"{DELTA_PATH}/bronze/machine_events"
SILVER_PATH = f"{DELTA_PATH}/silver/machine_events"
WINDOW_HOURS = 1
start_time = datetime.now() - timedelta(hours=WINDOW_HOURS)

# === Spark ===
spark = (
    SparkSession.builder
    .appName("SilverTransformMachineEvents")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

print(f"🚀 Starting Silver transformation — processing last {WINDOW_HOURS} hours...")

# === Leitura do Bronze ===
df_bronze = (
    spark.read.format("delta").load(BRONZE_PATH)
    .filter(col("timestamp") >= lit(start_time.isoformat()))
)

count_bronze = df_bronze.count()
print(f"📦 Found {count_bronze} events to process from Bronze.")

if count_bronze == 0:
    print("⚠️ No new data found. Exiting job.")
    spark.stop()
    exit(0)

# === Limpeza e enriquecimento ===
df_silver = (
    df_bronze
    .dropDuplicates(["event_id"])
    .na.drop(subset=["machine_id", "timestamp"])
    .withColumn("temperature", col("temperature").cast("double"))
    .withColumn("vibration", col("vibration").cast("double"))
    .withColumn("energy_kw", col("energy_kw").cast("double"))
    .withColumn("production_count", col("production_count").cast("int"))
    .withColumn("is_failure", when(col("status") == "FAILURE", True).otherwise(False))
    .withColumn("processed_at", current_timestamp())
    .withColumn("year", year(col("timestamp")))
    .withColumn("month", month(col("timestamp")))
    .withColumn("day", dayofmonth(col("timestamp")))
    .withColumn("hour", hour(col("timestamp")))
)

# === Escrita com MERGE (upsert) ===
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    print("🔁 Merging data into existing Silver table...")
    delta_table = DeltaTable.forPath(spark, SILVER_PATH)
    (
        delta_table.alias("tgt")
        .merge(
            df_silver.alias("src"),
            "tgt.event_id = src.event_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    print("🆕 Creating Silver table...")
    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month", "day", "hour")
        .save(SILVER_PATH)
    )

print(f"✅ Silver layer updated at {SILVER_PATH}")
spark.stop()
