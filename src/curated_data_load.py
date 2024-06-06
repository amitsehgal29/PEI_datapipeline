from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, round, year
from pyspark import StorageLevel

def load_staging_table(spark: SparkSession, table_name: str) -> DataFrame:
    return spark.read.table(f"stg.{table_name}")

def transform_orders(df_orders: DataFrame, df_customer: DataFrame, df_product: DataFrame) -> DataFrame:
    df_orders = df_orders.withColumn("PROFIT", round(col("PROFIT"), 2))

    df_curated_order_extended = df_orders \
        .join(df_customer, on="CUSTOMER_ID", how="inner") \
        .join(df_product, on="PRODUCT_ID", how="inner") \
        .select("ORDER_ID", "ORDER_DATE", "SHIP_DATE", "SHIP_MODE", "CUSTOMER_ID", 
                "CUSTOMER_NAME", "COUNTRY", "PRODUCT_ID", "CATEGORY", "SUB_CATEGORY", 
                "QUANTITY", "PRICE", "DISCOUNT", "PROFIT")

    df_curated_order_extended.persist(StorageLevel.MEMORY_AND_DISK)
    return df_curated_order_extended

def add_year_column(df: DataFrame) -> DataFrame:
    return df.withColumn("YEAR", year(col("ORDER_DATE")))

def aggregate_profits(df: DataFrame) -> DataFrame:
    return df.groupBy("YEAR", "CATEGORY", "SUB_CATEGORY", "CUSTOMER_ID") \
             .agg({"PROFIT": "sum"}) \
             .withColumnRenamed("sum(PROFIT)", "TOTAL_PROFIT")

def write_to_curated(df: DataFrame, table_name: str):
    df.write.format("delta").saveAsTable(f"curated.{table_name}")

def main():
    spark = SparkSession.builder.appName("DataTransform").getOrCreate()

    df_orders = load_staging_table(spark, "orders")
    df_product = load_staging_table(spark, "product")
    df_customer = load_staging_table(spark, "customer")

    df_curated_order_extended = transform_orders(df_orders, df_customer, df_product)
    write_to_curated(df_curated_order_extended, "order_extended")

    df_curated_order_extended = add_year_column(df_curated_order_extended)
    df_curated_agg_profits = aggregate_profits(df_curated_order_extended)
    write_to_curated(df_curated_agg_profits, "agg_profits")

if __name__ == "__main__":
    main()
