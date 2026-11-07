"""
Spark Session Setup for AdventureWorksDW2022 Lab
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date, year, month

def create_spark_session(app_name="AdventureWorksDW2022-PySpark-Lab"):
    """Create and configure Spark session with optimal settings"""
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def get_database_config():
    """Get database connection configuration"""
    return {
        "url": "jdbc:sqlserver://localhost:1433;database=AdventureWorksDW2022",
        "user": "your_username",
        "password": "your_password",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

def get_storage_config():
    """Get storage configuration"""
    return {
        "output_format": "parquet",
        "compression": "snappy",
        "partition_columns": ["OrderYear", "OrderMonth"]
    }