from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum, count, when, lit
from delta.tables import DeltaTable
from datetime import datetime, timedelta, timezone
import os

# === Configurações ===
SILVER_PATH = "/data/silver/machine_events"
GOLD_PATH = "/data/gold/machine_kpis"
LOOKBACK_HOURS = 1  # processa últimas 1h de silver

# === Inicializa Spark ===
spark = (
    SparkSession.builder.appName("GoldAggregateMachineKPIs")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

print(f"🚀 Starting Gold aggregation job at {datetime.now()}")

# === Carrega Silver ===
df_silver = spark.read.format("delta").load(SILVER_PATH)

# Filtra últimas 24h
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(hours=LOOKBACK_HOURS)
print(f"⏱ Filtering events from {start_time.isoformat()} to {end_time.isoformat()}")

df_silver = df_silver.filter(
    (col("timestamp") >= lit(start_time.isoformat())) &
    (col("timestamp") <= lit(end_time.isoformat()))
)

count_silver = df_silver.count()
print(f"📦 Found {count_silver} silver events to aggregate.")

if count_silver == 0:
    print("⚠️ No new data found. Exiting job.")
    spark.stop()
    exit(0)

# === Agregações ===
df_gold = (
    df_silver
    .groupBy("machine_id", "year", "month", "day", "hour")
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("vibration").alias("avg_vibration"),
        avg("energy_kw").alias("avg_energy_kw"),
        _sum("production_count").alias("total_production_count"),
        count(when(col("status") == "FAILURE", 1)).alias("failures"),
        count(when(col("status") == "ALERT", 1)).alias("alerts"),
        count(when(col("status") == "OK", 1)).alias("ok_events")
    )
    .withColumn("efficiency", col("total_production_count") / (col("failures") + col("alerts") + lit(1)))
)

# === Escrita incremental ===
gold_log = f"{GOLD_PATH}/_delta_log"

if os.path.exists(gold_log):
    print(f"🔁 Merging into existing Gold table at {GOLD_PATH}")
    delta_table = DeltaTable.forPath(spark, GOLD_PATH)
    (
        delta_table.alias("tgt")
        .merge(
            df_gold.alias("src"),
            """
            tgt.machine_id = src.machine_id AND
            tgt.year = src.year AND
            tgt.month = src.month AND
            tgt.day = src.day AND
            tgt.hour = src.hour
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    print(f"🆕 Creating new Gold table at {GOLD_PATH}")
    (
        df_gold.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("year", "month", "day", "hour")
        .save(GOLD_PATH)
    )

# === Logs e encerramento ===
delta = DeltaTable.forPath(spark, GOLD_PATH)
total_rows = delta.toDF().count()
print(f"✅ Gold table updated successfully. Total rows: {total_rows}")
spark.stop()
