"""
DuckDB Integration for Advanced Analytics and Versioning
Demonstrates SCD Type 3 replacement using DuckDB merge operations
"""

import duckdb
import pandas as pd
import os

def setup_duckdb_environment():
    """Setup DuckDB database and tables for analytics"""
    
    # Connect to DuckDB (creates file if it doesn't exist)
    con = duckdb.connect('data/analytics.duckdb')
    
    print("Connected to DuckDB database")
    
    # Create sample tables for demonstration
    try:
        # Create a customer table with versioning
        con.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                CustomerKey INTEGER,
                CustomerID VARCHAR,
                FirstName VARCHAR,
                LastName VARCHAR,
                FullName VARCHAR,
                BirthDate DATE,
                Age INTEGER,
                AnnualIncome DECIMAL(10,2),
                HouseOwnerFlag BOOLEAN,
                NumberCarsOwned INTEGER,
                AddressLine1 VARCHAR,
                City VARCHAR,
                StateProvinceName VARCHAR,
                CountryRegionName VARCHAR,
                version INTEGER DEFAULT 1,
                valid_from DATE,
                valid_to DATE
            )
        """)
        
        # Create a product table  
        con.execute("""
            CREATE TABLE IF NOT EXISTS products (
                ProductKey INTEGER,
                ProductID VARCHAR,
                ProductName VARCHAR,
                ProductLine VARCHAR,
                Size VARCHAR,
                Weight DECIMAL(10,2),
                Cost DECIMAL(10,2),
                ListPrice DECIMAL(10,2),
                Status VARCHAR,
                version INTEGER DEFAULT 1
            )
        """)
        
        # Create a sales fact table
        con.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                SalesOrderNumber VARCHAR,
                SalesOrderLineNumber INTEGER,
                OrderDateKey INTEGER,
                OrderDate DATE,
                ProductKey INTEGER,
                CustomerKey INTEGER,
                SalesAmount DECIMAL(10,2),
                OrderQuantity INTEGER,
                TotalProductCost DECIMAL(10,2),
                version INTEGER DEFAULT 1
            )
        """)
        
        print("Created analytics tables in DuckDB")
        
    except Exception as e:
        print(f"Error creating tables: {e}")
        
    return con

def demonstrate_scd_operations(con):
    """Demonstrate SCD Type 3 replacement using DuckDB"""
    
    print("\n--- SCD Type 3 Operations Demo ---")
    
    # Example of upsert operation (merge)
    try:
        # Insert sample data
        con.execute("""
            INSERT INTO customers 
            VALUES (1, 'AW0000001', 'John', 'Doe', 'John Doe', '1980-01-01', 43, 75000.00, TRUE, 2, '123 Main St', 'Seattle', 'Washington', 'United States', 1, '2023-01-01', NULL)
        """)
        
        print("Inserted sample customer data")
        
        # Update operation with versioning (this demonstrates how we'd handle SCD Type 3)
        con.execute("""
            INSERT INTO customers 
            VALUES (1, 'AW0000001', 'John', 'Doe', 'John Doe', '1980-01-01', 43, 80000.00, TRUE, 2, '123 Main St', 'Seattle', 'Washington', 'United States', 2, '2023-06-01', NULL)
        """)
        
        print("Updated customer data with new version")
        
        # Query to show versions
        result = con.execute("SELECT * FROM customers ORDER BY version").fetchdf()
        print("\nCustomer data with versions:")
        print(result)
        
    except Exception as e:
        print(f"Error in SCD operations: {e}")

def demonstrate_analytics_queries(con):
    """Demonstrate advanced analytics queries using DuckDB"""
    
    print("\n--- Analytics Queries ---")
    
    try:
        # Sample analytics query
        result = con.execute("""
            SELECT 
                c.StateProvinceName,
                COUNT(*) as customer_count,
                AVG(c.AnnualIncome) as avg_income,
                COUNT(DISTINCT c.CustomerKey) as unique_customers
            FROM customers c
            WHERE c.version = 1
            GROUP BY c.StateProvinceName
            ORDER BY avg_income DESC
        """).fetchdf()
        
        print("Customer analytics by state:")
        print(result)
        
    except Exception as e:
        print(f"Error in analytics queries: {e}")

def main():
    """Main DuckDB integration demonstration"""
    print("Starting DuckDB Integration Demo...")
    
    # Setup environment
    con = setup_duckdb_environment()
    
    # Demonstrate operations
    demonstrate_scd_operations(con)
    demonstrate_analytics_queries(con)
    
    # Close connection
    con.close()
    print("\nDuckDB integration demo completed!")

if __name__ == "__main__":
    main()