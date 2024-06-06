


def load_json(path) -> DataFrame:
    try:
        return spark.read.json(path)
    except Exception as e:
        raise RuntimeError(f"Failed to load JSON file: {e}")

def load_excel(path, header: str= "true")-> DataFrame:
    try:
        return spark.read.format("com.crealytics.spark.excel").option("header", "true").load(path)
    except Exception as e:
        raise RuntimeError(f"Failed to load Excel file: {e}")

def load_csv(path)->DataFrame:
    try:
        return spark.read.csv(path, header=True)
    except Exception as e:
        raise RuntimeError(f"Failed to load CSV file: {e}")

def write_to_delta(df, table_name)->None:
    try:
        df.write.format("delta").saveAsTable(table_name)
    except Exception as e:
        raise RuntimeError(f"Failed to write to Delta table: {e}")
        
        

# Define file paths
order_path = "/path/to/Order.json"
customer_path = "/path/to/Customer.xlsx"
product_path = "/path/to/Product.csv"

# Load datasets
order_df = load_json(order_path)

customer_df = load_excel(customer_df)

product_df = load_csv(product_df)

# Write the df as delta tables in raw layer
write_to_delta(df_order, "raw.order")
write_to_delta(df_customer, "raw.customer")
write_to_delta(df_product, "raw.product")

