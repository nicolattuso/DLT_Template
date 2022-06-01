import dlt  # pylint: disable=import-error

from dlt_pipeline.data_quality_checks import (
    enriched_taxi_data_valid,
    enriched_taxi_data_warn,
    raw_taxi_data_valid,
    raw_taxi_data_warn,
)
from dlt_pipeline.transformations import enrich_nyc_taxi_data


def define_pipeline(spark):
    @dlt.table(comment="NYC Taxi data")
    @dlt.expect_all_or_drop(raw_taxi_data_valid)
    @dlt.expect_all(raw_taxi_data_warn)
    def raw_nyc_taxi_data():
        nyc_taxi_df = spark.read.format("json").load(
            "/databricks-datasets/nyctaxi/sample/json/"
        )
        return nyc_taxi_df

    @dlt.table(comment="Enriched NYC Taxi data")
    @dlt.expect_all_or_drop(enriched_taxi_data_valid)
    @dlt.expect_all(enriched_taxi_data_warn)
    def enriched_nyc_taxi_data():
        nyc_taxi_df = dlt.read("raw_nyc_taxi_data")
        return enrich_nyc_taxi_data(nyc_taxi_df)
