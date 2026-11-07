"""
PySpark ETL Pipeline for AdventureWorksDW2022 Data Engineering Lab
This demonstrates the same patterns using PySpark instead of Python/pandas
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date, year, month, sum as spark_sum, count as spark_count
from pyspark.sql.types import *
import os

def create_spark_session():
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName("AdventureWorksDW2022-PySpark-Lab") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    return spark

def extract_data(spark, jdbc_url, db_properties):
    """Extract data from AdventureWorksDW2022 database"""
    
    # Extract Customer data
    customer_df = spark.read.jdbc(
        url=jdbc_url,
        table="DimCustomer",
        properties=db_properties
    )
    
    # Extract Product data
    product_df = spark.read.jdbc(
        url=jdbc_url,
        table="DimProduct",
        properties=db_properties
    )
    
    # Extract Sales data
    sales_df = spark.read.jdbc(
        url=jdbc_url,
        table="FactInternetSales",
        properties=db_properties
    )
    
    return customer_df, product_df, sales_df

def transform_data(spark, customer_df, product_df, sales_df):
    """Transform data using PySpark DataFrame operations"""
    
    # Join tables to create fact table
    joined_df = sales_df.join(
        customer_df,
        sales_df["CustomerKey"] == customer_df["CustomerKey"],
        "inner"
    ).join(
        product_df,
        sales_df["ProductKey"] == product_df["ProductKey"],
        "inner"
    )
    
    # Add calculated columns
    transformed_df = joined_df.withColumn(
        "OrderYear", year(to_date(col("OrderDate")))
    ).withColumn(
        "OrderMonth", month(to_date(col("OrderDate")))
    ).withColumn(
        "TotalAmount", col("SalesAmount") * col("Quantity")
    )
    
    # Group by customer and product for aggregated analysis
    summary_df = transformed_df.groupBy(
        "CustomerKey",
        "ProductKey",
        "OrderYear",
        "OrderMonth"
    ).agg(
        spark_sum("SalesAmount").alias("TotalSales"),
        spark_sum("Quantity").alias("TotalQuantity"),
        spark_count("*").alias("TransactionCount")
    )
    
    return summary_df

def load_data(spark, df, output_path):
    """Load data to Parquet with partitioning"""
    
    # Write to Parquet with partitioning
    df.coalesce(4).write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(output_path)
    
    return True

def main():
    """Main PySpark ETL pipeline"""
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Database connection parameters
        jdbc_url = "jdbc:sqlserver://localhost:1433;database=AdventureWorksDW2022"
        db_properties = {
            "user": "your_username",
            "password": "your_password",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
        
        # Extract data
        print("Extracting data from AdventureWorksDW2022...")
        customer_df, product_df, sales_df = extract_data(spark, jdbc_url, db_properties)
        
        # Transform data
        print("Transforming data...")
        summary_df = transform_data(spark, customer_df, product_df, sales_df)
        
        # Load data
        output_path = "hdfs://namenode:9000/data/processed/adventureworks_sales"
        print(f"Loading data to {output_path}...")
        load_data(spark, summary_df, output_path)
        
        print("PySpark ETL pipeline completed successfully!")
        
    except Exception as e:
        print(f"Error in PySpark pipeline: {str(e)}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()