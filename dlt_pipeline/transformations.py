import pyspark.sql.functions as F


def enrich_nyc_taxi_data(nyc_taxi_df):
    return nyc_taxi_df.withColumn(
        "short_ride", F.when(F.col("trip_distance") < 1, True).otherwise(False)
    )
