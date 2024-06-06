# Project Name: PySpark Data Pipeline

## Overview
This project is a PySpark data pipeline designed to process and transform raw data from various sources into curated datasets. The pipeline follows a Test-Driven Development (TDD) approach, ensuring reliability and accuracy throughout the development process.

## Data Sources
The initial data sources include:
- **Orders:** Contains information about orders placed, including order ID, dates, shipping details, customer ID, product ID, quantity, price, discount, and profit.
- **Product:** Contains details about products, including product ID, category, sub-category, product name, state, and price per product.
- **Customer:** Holds customer information such as customer ID, name, email, phone, address, segment, country, city, state, postal code, and region.

## Project Structure
The project structure consists of the following components:

### 1. src Folder
- **raw_data_load.py:** Python script to load raw data from various file formats such as JSON, CSV, and Excel. It provides functions for reading these files into PySpark DataFrames.
- **stg_data_load.py:** Script to transform raw data into staged data, standardizing column names, replacing spaces with underscores, removing duplicates, and dropping null values from customer and product tables.
- **curated_data_load.py:** Module responsible for loading staged data into curated datasets. It involves joining all three tables, creating a curated_order_extended table, and aggregating profits. 
- **SQL:** File containing SQL statements for data aggregation purposes.

### 2. test_Cases Folder
- **test_raw_data_load.py:** Unit tests for the `raw_data_load.py` module, ensuring functionality and handling of exceptions when files are incorrect or missing.
- **test_stg_data_load.py:** Unit tests validating the functionality of the `stg_data_load.py` module.
- **test_curated_data_load.py:** Unit tests for the `curated_data_load.py` module, checking its functionality and correctness.


## Dependencies
- Python 3.x
- PySpark
- Pytest (for running tests)


