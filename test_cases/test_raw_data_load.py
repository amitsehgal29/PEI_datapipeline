import pytest
from pyspark.sql import SparkSession
from src.raw_data_load import load_json, load_excel, load_csv, write_to_delta

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("TestDataIngestion") \
        .master("local[*]") \
        .getOrCreate()

def test_load_json_success(spark):
    df = load_json(spark, "/test_data/Order.json")
    assert df is not None
    assert df.count() > 0

def test_load_excel_success(spark):
    df = load_excel(spark, "/test_data/Customer.xlsx")
    assert df is not None
    assert df.count() > 0

def test_load_csv_success(spark):
    df = load_csv(spark, "/test_data/Product.csv")
    assert df is not None
    assert df.count() > 0

def test_load_non_existent_file(spark):
    with pytest.raises(RuntimeError):
        load_json(spark, "/test_data/NonExistentFile.json")

def test_load_corrupt_file(spark):
    with pytest.raises(RuntimeError):
        load_json(spark, "/test_data/CorruptFile.json")

def test_write_to_delta(spark):
    df = spark.createDataFrame([(1, "sample")], ["id", "value"])
    write_to_delta(df, "raw.test_table")
    result = spark.sql("SHOW TABLES IN raw LIKE 'test_table'")
    assert result.count() == 1
    assert result.collect()[0]['tableName'] == 'test_table'
