"""
Database utilities for connecting to AdventureWorksDW2022 database
"""

import pyodbc
import pandas as pd
from typing import Optional, List, Union

class DatabaseConnector:
    """Utility class for connecting to SQL Server and executing queries"""
    
    def __init__(self, server_name: str = "maingear", database_name: str = "AdventureWorksDW2022"):
        self.server_name = server_name
        self.database_name = database_name
        self.connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server_name};"
            f"DATABASE={database_name};"
            f"Trusted_Connection=yes;"
        )
    
    def get_connection(self) -> pyodbc.Connection:
        """Get a database connection"""
        return pyodbc.connect(self.connection_string)
    
    def execute_query(self, query: str, parameters: Optional[tuple] = None) -> pd.DataFrame:
        """Execute a SQL query and return as DataFrame"""
        try:
            conn = self.get_connection()
            df = pd.read_sql(query, conn, params=parameters)
            conn.close()
            return df
        except Exception as e:
            print(f"Error executing query: {e}")
            raise
    
    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """Get the schema of a specific table"""
        query = """
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        return self.execute_query(query, (table_name,))
    
    def get_all_table_names(self) -> List[str]:
        """Get all table names in the database"""
        query = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        df = self.execute_query(query)
        return df['TABLE_NAME'].tolist()

# Example usage:
if __name__ == "__main__":
    # Initialize the database connector
    db = DatabaseConnector()
    
    # Get all table names
    tables = db.get_all_table_names()
    print("Available tables:")
    for table in tables:
        print(f"  - {table}")