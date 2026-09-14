import argparse
from databricks.sdk.runtime import spark
from bundleTest import taxis
from pyspark.sql import functions as F


def main():
    # Process command-line arguments
    parser = argparse.ArgumentParser(
        description="Databricks job with catalog and schema parameters",
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    # Set the default catalog and schema
    spark.sql(f"USE CATALOG `{args.catalog}`")
    spark.sql(f"USE SCHEMA `{args.schema}`")

    # Example: just find all taxis from a sample catalog
    #taxis.find_all_taxis().show(10)
    #taxis.find_expensive_taxis().show(10)

    df = taxis.find_expensive_taxis()

    #result = (
    #df.groupBy("pickup_zip")
    #  .count()
    #  .orderBy("count", ascending=False)
    #)

    result = df.groupBy("pickup_zip").agg(
    F.count("*").alias("trip_count"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare")
    )

    result.show(10)


    result.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        f"{args.catalog}.{args.schema}.top_pickup_zips"
    )


if __name__ == "__main__":
    main()
