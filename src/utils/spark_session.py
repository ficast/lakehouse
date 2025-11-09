from pyspark.sql import SparkSession
import os

def get_spark(app_name="LakehouseApp"):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    if os.getenv("SPARK_ENV", "local") == "local":
        builder = builder.master("local[*]")

    spark = builder.getOrCreate()
    return spark
