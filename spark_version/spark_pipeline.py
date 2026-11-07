"""
PySpark ETL Pipeline for AdventureWorksDW2022 Data Engineering Lab
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date, year, month, sum as spark_sum, count as spark_count
from pyspark.sql.types import *
from spark_setup import create_spark_session, get_database_config, get_storage_config

def extract_data(spark):
    """Extract data from AdventureWorksDW2022 database using Spark JDBC"""
    
    db_config = get_database_config()
    
    # Extract Customer data  
    customer_df = spark.read.jdbc(
        url=db_config["url"],
        table="DimCustomer",
        properties=db_config
    )
    
    # Extract Product data
    product_df = spark.read.jdbc(
        url=db_config["url"], 
        table="DimProduct",
        properties=db_config
    )
    
    # Extract Sales data
    sales_df = spark.read.jdbc(
        url=db_config["url"],
        table="FactInternetSales",
        properties=db_config
    )
    
    print(f"Customer data: {customer_df.count()} rows")
    print(f"Product data: {product_df.count()} rows") 
    print(f"Sales data: {sales_df.count()} rows")
    
    return customer_df, product_df, sales_df

def transform_data(spark, customer_df, product_df, sales_df):
    """Transform data using PySpark DataFrame operations"""
    
    # Join tables to create fact table (using Spark's optimized joins)
    joined_df = sales_df.join(
        customer_df,
        sales_df["CustomerKey"] == customer_df["CustomerKey"],
        "inner"
    ).join(
        product_df,
        sales_df["ProductKey"] == product_df["ProductKey"],
        "inner"
    )
    
    # Add calculated columns using Spark functions
    transformed_df = joined_df.withColumn(
        "OrderYear", year(to_date(col("OrderDate")))
    ).withColumn(
        "OrderMonth", month(to_date(col("OrderDate")))
    ).withColumn(
        "TotalAmount", col("SalesAmount") * col("Quantity")
    )
    
    # Create summary aggregates using Spark's optimized aggregation
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
    
    print(f"Summary data: {summary_df.count()} rows")
    
    return summary_df

def load_data(spark, df, output_path):
    """Load data to Parquet with partitioning and optimal configuration"""
    
    storage_config = get_storage_config()
    
    # Write to Parquet with partitioning for better query performance
    df.coalesce(4).write \
        .mode("overwrite") \
        .option("compression", storage_config["compression"]) \
        .partitionBy("OrderYear", "OrderMonth") \
        .parquet(output_path)
    
    print(f"Data loaded to {output_path}")
    return True

def main():
    """Main PySpark ETL pipeline"""
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Extract data
        print("Extracting data from AdventureWorksDW2022...")
        customer_df, product_df, sales_df = extract_data(spark)
        
        # Transform data
        print("Transforming data with PySpark...")
        summary_df = transform_data(spark, customer_df, product_df, sales_df)
        
        # Load data
        output_path = "hdfs://namenode:9000/data/processed/adventureworks_sales_spark"
        print(f"Loading data to {output_path}...")
        load_data(spark, summary_df, output_path)
        
        print("PySpark ETL pipeline completed successfully!")
        
        # Show sample data
        summary_df.show(5)
        
    except Exception as e:
        print(f"Error in PySpark pipeline: {str(e)}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()