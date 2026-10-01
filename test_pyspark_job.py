import pytest
from pyspark.sql import SparkSession

from pyspark_job import clean_data


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("TestCleanData")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_valid_records_are_kept(spark):
    data = [
        ("Ahmed", 100),
        ("Sara", 50)
    ]

    df = spark.createDataFrame(data, ["name", "amount"])

    result = clean_data(df)

    assert result.count() == 2


def test_amount_less_than_or_equal_to_zero_removed(spark):
    data = [
        ("Ahmed", 100),
        ("Sara", 0),
        ("Omar", -20)
    ]

    df = spark.createDataFrame(data, ["name", "amount"])

    result = clean_data(df)

    assert result.count() == 1
    assert result.first()["name"] == "Ahmed"


def test_null_names_removed(spark):
    data = [
        ("Ahmed", 100),
        (None, 200)
    ]

    df = spark.createDataFrame(data, ["name", "amount"])

    result = clean_data(df)

    assert result.count() == 1
    assert result.first()["name"] == "Ahmed"


def test_amount_with_tax_calculated_correctly(spark):
    data = [
        ("Ahmed", 100),
        ("Sara", 50)
    ]

    df = spark.createDataFrame(data, ["name", "amount"])

    result = clean_data(df)

    rows = result.collect()

    assert rows[0]["amount_with_tax"] == 120.0
    assert rows[1]["amount_with_tax"] == 60.0
