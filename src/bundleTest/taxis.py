from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame


def find_all_taxis() -> DataFrame:
    """Find all taxi data."""
    return spark.read.table("samples.nyctaxi.trips")

def find_expensive_taxis() -> DataFrame:
    """Find taxi trips with a fare greater than $20."""

    return (
        spark.read.table("samples.nyctaxi.trips")
        .filter("fare_amount > 20")
    )