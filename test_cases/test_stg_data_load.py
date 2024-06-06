import pytest
from pyspark.sql import SparkSession
from src.stg_data_load import (
    rename_columns, remove_duplicates, handle_missing_values, 
    load_raw_table, write_to_staging, transform_orders, transform_product, transform_customer
)

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("TestDataTransform") \
        .master("local[*]") \
        .getOrCreate()

@pytest.fixture(scope="module")
def raw_data(spark):
    # Mock raw data
    df_orders = spark.read.json("/test_data/raw_orders.json")
    df_product = spark.read.json("/test_data/raw_product.json")
    df_customer = spark.read.json("/test_data/raw_customer.json")
    
    return df_orders, df_product, df_customer

def test_rename_columns(raw_data):
    df_orders, _, _ = raw_data
    df_orders = rename_columns(df_orders)
    
    assert all(col.isupper() and ' ' not in col for col in df_orders.columns)

def test_remove_duplicates(raw_data):
    _, df_product, _ = raw_data
    df_product = remove_duplicates(df_product)
    
    assert df_product.count() == df_product.dropDuplicates().count()

def test_handle_missing_values(raw_data):
    _, df_product, _ = raw_data
    df_product = handle_missing_values(df_product)
    
    assert df_product.filter("value IS NULL").count() == 0

def test_transform_orders(raw_data):
    df_orders, _, _ = raw_data
    df_orders = transform_orders(df_orders)
    
    assert all(col.isupper() and ' ' not in col for col in df_orders.columns)

def test_transform_product(raw_data):
    _, df_product, _ = raw_data
    df_product = transform_product(df_product)
    
    assert all(col.isupper() and ' ' not in col for col in df_product.columns)
    assert df_product.count() == df_product.dropDuplicates().count()
    assert df_product.filter("value IS NULL").count() == 0

def test_transform_customer(raw_data):
    _, _, df_customer = raw_data
    df_customer = transform_customer(df_customer)
    
    assert all(col.isupper() and ' ' not in col for col in df_customer.columns)
    assert df_customer.count() == df_customer.dropDuplicates().count()
    assert df_customer.filter("value IS NULL").count() == 0

def test_write_to_staging(spark, raw_data):
    df_orders, df_product, df_customer = raw_data
    df_orders = transform_orders(df_orders)
    df_product = transform_product(df_product)
    df_customer = transform_customer(df_customer)
    
    write_to_staging(df_orders, "orders")
    write_to_staging(df_product, "product")
    write_to_staging(df_customer, "customer")
    
    result_orders = spark.sql("SHOW TABLES IN stg LIKE 'orders'")
    result_product = spark.sql("SHOW TABLES IN stg LIKE 'product'")
    result_customer = spark.sql("SHOW TABLES IN stg LIKE 'customer'")
    
    assert result_orders.count() == 1
    assert result_orders.collect()[0]['tableName'] == 'orders'
    
    assert result_product.count() == 1
    assert result_product.collect()[0]['tableName'] == 'product'
    
    assert result_customer.count() == 1
    assert result_customer.collect()[0]['tableName'] == 'customer'
