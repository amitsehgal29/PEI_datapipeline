from pyspark.sql import SparkSession, DataFrame

def rename_columns(df: DataFrame) -> DataFrame:
    for column in df.columns:
        df = df.withColumnRenamed(column, column.upper().replace(' ', '_'))
    return df

def remove_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates()

def handle_missing_values(df: DataFrame) -> DataFrame:
    return df.na.drop()

def load_raw_table(spark: SparkSession, table_name: str) -> DataFrame:
    return spark.read.table(table_name)

def write_to_staging(df: DataFrame, table_name: str):
    df.write.format("delta").saveAsTable(f"stg.{table_name}")

def transform_orders(df: DataFrame) -> DataFrame:
    return rename_columns(df)

def transform_product(df: DataFrame) -> DataFrame:
    df = rename_columns(df)
    df = remove_duplicates(df)
    df = handle_missing_values(df)
    return df

def transform_customer(df: DataFrame) -> DataFrame:
    df = rename_columns(df)
    df = remove_duplicates(df)
    df = handle_missing_values(df)
    return df

def main():
    spark = SparkSession.builder.appName("DataTransform").getOrCreate()

    df_orders = load_raw_table(spark, "raw.orders")
    df_product = load_raw_table(spark, "raw.product")
    df_customer = load_raw_table(spark, "raw.customer")

    df_orders = transform_orders(df_orders)
    df_product = transform_product(df_product)
    df_customer = transform_customer(df_customer)

    write_to_staging(df_orders, "orders")
    write_to_staging(df_product, "product")
    write_to_staging(df_customer, "customer")

if __name__ == "__main__":
    main()
