import dlt  # pylint: disable=import-error

from dlt_pipeline.transformations import enrich_nyc_taxi_data


def define_pipeline(spark):
    @dlt.table(comment="NYC Taxi data")
    def raw_nyc_taxi_data():
        nyc_taxi_df = spark.read.format("json").load(
            "/databricks-datasets/nyctaxi/sample/json/"
        )
        return nyc_taxi_df

    @dlt.table(comment="Enriched NYC Taxi data")
    def enriched_nyc_taxi_data():
        nyc_taxi_df = dlt.read("raw_nyc_taxi_data")
        return enrich_nyc_taxi_data(nyc_taxi_df)
