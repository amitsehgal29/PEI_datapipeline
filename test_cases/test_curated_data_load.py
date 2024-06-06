import pytest
from pyspark.sql import SparkSession
from src.curated_data_load import (
    load_staging_table, transform_orders, add_year_column, aggregate_profits, write_to_curated
)

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("TestDataTransform") \
        .master("local[*]") \
        .getOrCreate()

@pytest.fixture(scope="module")
def staging_data(spark):
    # Mock staging data
    df_orders = spark.read.json("/test_data/stg_orders.json")
    df_product = spark.read.json("/test_data/stg_product.json")
    df_customer = spark.read.json("/test_data/stg_customer.json")
    
    return df_orders, df_product, df_customer

def test_transform_orders(staging_data):
    df_orders, df_product, df_customer = staging_data
    df_curated_order_extended = transform_orders(df_orders, df_customer, df_product)
    
    expected_columns = [
        "ORDER_ID", "ORDER_DATE", "SHIP_DATE", "SHIP_MODE", "CUSTOMER_ID", 
        "CUSTOMER_NAME", "COUNTRY", "PRODUCT_ID", "CATEGORY", "SUB_CATEGORY", 
        "QUANTITY", "PRICE", "DISCOUNT", "PROFIT"
    ]
    assert set(df_curated_order_extended.columns) == set(expected_columns)
    assert df_curated_order_extended.filter(df_curated_order_extended["PROFIT"].isNull()).count() == 0

def test_add_year_column(staging_data):
    df_orders, df_product, df_customer = staging_data
    df_curated_order_extended = transform_orders(df_orders, df_customer, df_product)
    df_curated_order_extended = add_year_column(df_curated_order_extended)
    
    assert "YEAR" in df_curated_order_extended.columns
    assert df_curated_order_extended.filter(df_curated_order_extended["YEAR"].isNull()).count() == 0

def test_aggregate_profits(staging_data):
    df_orders, df_product, df_customer = staging_data
    df_curated_order_extended = transform_orders(df_orders, df_customer, df_product)
    df_curated_order_extended = add_year_column(df_curated_order_extended)
    df_curated_agg_profits = aggregate_profits(df_curated_order_extended)
    
    expected_columns = ["YEAR", "CATEGORY", "SUB_CATEGORY", "CUSTOMER_ID", "TOTAL_PROFIT"]
    assert set(df_curated_agg_profits.columns) == set(expected_columns)
    assert df_curated_agg_profits.filter(df_curated_agg_profits["TOTAL_PROFIT"].isNull()).count() == 0

def test_write_to_curated(spark, staging_data):
    df_orders, df_product, df_customer = staging_data
    df_curated_order_extended = transform_orders(df_orders, df_customer, df_product)
    write_to_curated(df_curated_order_extended, "order_extended")

    result_orders = spark.sql("SHOW TABLES IN curated LIKE 'order_extended'")
    assert result_orders.count() == 1
    assert result_orders.collect()[0]['tableName'] == 'order_extended'

    df_curated_order_extended = add_year_column(df_curated_order_extended)
    df_curated_agg_profits = aggregate_profits(df_curated_order_extended)
    write_to_curated(df_curated_agg_profits, "agg_profits")

    result_profits = spark.sql("SHOW TABLES IN curated LIKE 'agg_profits'")
    assert result_profits.count() == 1
    assert result_profits.collect()[0]['tableName'] == 'agg_profits'
