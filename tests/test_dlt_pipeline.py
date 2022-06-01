# pylint: disable=redefined-outer-name
import pytest
from pyspark.sql import SparkSession

from dlt_pipeline import __version__
from dlt_pipeline.transformations import enrich_nyc_taxi_data


@pytest.fixture()
def spark():
    spark_session = (
        SparkSession.builder.master("local[*]")
        .appName("dlt-pipeline-tests")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def format_test_dataframe(dataframe):
    return dataframe.toJSON().collect()


def test_version():
    assert __version__ == "0.1.0"


def test_enrich_nyc_taxi_data(spark):
    nyc_taxi_data = [("trip 1", 0.9), ("trip 2", 1.0), ("trip 3", 1.1)]
    nyc_taxi_columns = ["trip_id", "trip_distance"]
    input_nyc_taxi_df = spark.createDataFrame(nyc_taxi_data).toDF(*nyc_taxi_columns)

    expected_enriched_nyc_taxi_data = [
        ("trip 1", 0.9, True),
        ("trip 2", 1.0, False),
        ("trip 3", 1.1, False),
    ]
    expected_enriched_nyc_taxi_columns = ["trip_id", "trip_distance", "short_ride"]
    expected_enriched_nyc_taxi_df = spark.createDataFrame(
        expected_enriched_nyc_taxi_data
    ).toDF(*expected_enriched_nyc_taxi_columns)

    enriched_nyc_taxi_df = enrich_nyc_taxi_data(input_nyc_taxi_df)

    assert format_test_dataframe(enriched_nyc_taxi_df) == format_test_dataframe(
        expected_enriched_nyc_taxi_df
    )
