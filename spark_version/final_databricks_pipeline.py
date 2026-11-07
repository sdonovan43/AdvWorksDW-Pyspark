"""
Final PySpark ETL Pipeline for AdventureWorksDW2022 Data Engineering Lab - Databricks Optimized
This is the clean, final version ready for your AdvWorksDW-Pyspark repository
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date, year, month, sum as spark_sum, count as spark_count

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
    # For Databricks Community Edition, update these with your actual credentials
    return {
        "url": "jdbc:sqlserver://maingear:1433;database=AdventureWorksDW2022",
        "user": "your_username",  # REPLACE WITH ACTUAL CREDENTIALS
        "password": "your_password",  # REPLACE WITH ACTUAL CREDENTIALS  
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

def extract_data(spark):
    """Extract data from AdventureWorksDW2022 database using Spark JDBC"""
    
    db_config = get_database_credentials()
    
    print("Extracting DimCustomer table...")
    customer_df = spark.read.jdbc(
        url=db_config["url"],
        table="DimCustomer",
        properties=db_config
    )
    
    print("Extracting DimProduct table...")
    product_df = spark.read.jdbc(
        url=db_config["url"], 
        table="DimProduct",
        properties=db_config
    )
    
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
        "TotalAmount", col("OrderQuantity") * col("UnitPrice")
    )
    
    # Aggregate data by customer and product
    print("Aggregating data...")
    result_df = transformed_df.groupBy(
        col("CustomerKey"),
        col("ProductKey"), 
        col("OrderYear"),
        col("OrderMonth")
    ).agg(
        spark_sum(col("TotalAmount")).alias("TotalSales"),
        spark_sum(col("OrderQuantity")).alias("TotalQuantity"),
        count(col("*")).alias("OrderCount")
    )
    
    return result_df

def load_data(spark, df):
    """Load transformed data to DBFS (Databricks File System)"""
    # Output path for Databricks
    output_path = "/tmp/adventureworks_sales_summary"
    
    print(f"Saving results to {output_path}")
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
    
    print("Data saved successfully!")
    return output_path

def main():
    """Main execution function"""
    try:
        # Initialize Spark session
        spark = create_spark_session()
        print("Spark session initialized successfully!")
        
        # Extract data
        customer_df, product_df, sales_df = extract_data(spark)
        
        # Transform data
        result_df = transform_data(spark, customer_df, product_df, sales_df)
        
        # Load data
        output_path = load_data(spark, result_df)
        
        print(f"ETL Process completed successfully!")
        print(f"Results stored at: {output_path}")
        
    except Exception as e:
        print(f"Error in ETL process: {str(e)}")
        raise
    
    finally:
        # Stop Spark session
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()