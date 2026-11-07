"""
PySpark ETL Pipeline for AdventureWorksDW2022 Data Engineering Lab - Databricks Optimized
This version is optimized specifically for Databricks Community Edition
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date, year, month, sum as spark_sum, count as spark_count
from pyspark.sql.types import *

def create_spark_session():
    """Create and configure Spark session optimized for Databricks"""
    spark = SparkSession.builder \
        .appName("AdventureWorksDW2022-Databricks-Lab") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    return spark

def get_database_credentials():
    """Get database connection credentials"""
    # For Databricks, these would typically come from Secret Stores or cluster configuration
    # In a real scenario, use Databricks Secrets or Environment Variables
    return {
        "url": "jdbc:sqlserver://maingear:1433;database=AdventureWorksDW2022",
        "user": "your_username",  # Replace with actual credentials
        "password": "your_password",  # Replace with actual credentials  
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

def extract_data(spark):
    """Extract data from AdventureWorksDW2022 database using Spark JDBC"""
    
    db_config = get_database_credentials()
    
    # Extract Customer data  
    print("Extracting DimCustomer table...")
    customer_df = spark.read.jdbc(
        url=db_config["url"],
        table="DimCustomer",
        properties=db_config
    )
    
    # Extract Product data
    print("Extracting DimProduct table...")
    product_df = spark.read.jdbc(
        url=db_config["url"], 
        table="DimProduct",
        properties=db_config
    )
    
    # Extract Sales data
    print("Extracting FactInternetSales table...")
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
    
    # Join tables to create fact table
    print("Joining tables...")
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
    print("Adding calculated columns...")
    transformed_df = joined_df.withColumn(
        "OrderYear", year(to_date(col("OrderDate")))
    ).withColumn(
        "OrderMonth", month(to_date(col("OrderDate")))
    ).withColumn(
        "TotalAmount", col("SalesAmount") * col("Quantity")
    )
    
    # Create summary aggregates 
    print("Creating summary aggregates...")
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

def load_data(spark, df):
    """Load data to Parquet in Databricks file system"""
    
    # Use DBFS path for Databricks (works with Databricks Community Edition)
    output_path = "/tmp/adventureworks_sales_summary"
    
    # Write to Parquet with partitioning for better query performance
    df.coalesce(4).write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("OrderYear", "OrderMonth") \
        .parquet(output_path)
    
    print(f"Data loaded to {output_path}")
    return True

def main():
    """Main PySpark ETL pipeline optimized for Databricks"""
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        print("Starting AdventureWorksDW2022 PySpark ETL Pipeline...")
        
        # Extract data
        print("Step 1: Extracting data from AdventureWorksDW2022...")
        customer_df, product_df, sales_df = extract_data(spark)
        
        # Transform data
        print("Step 2: Transforming data with PySpark...")
        summary_df = transform_data(spark, customer_df, product_df, sales_df)
        
        # Load data
        print("Step 3: Loading data to Databricks File System...")
        load_data(spark, summary_df)
        
        print("PySpark ETL pipeline completed successfully!")
        
        # Show sample data - this works in Databricks UI
        print("Sample data from processed dataset:")
        try:
            summary_df.show(5)
        except Exception as e:
            print(f"Could not display sample data: {e}")
        
    except Exception as e:
        print(f"Error in PySpark pipeline: {str(e)}")
        raise
    
    finally:
        # Clean up
        spark.stop()
        print("Spark session closed.")

if __name__ == "__main__":
    main()