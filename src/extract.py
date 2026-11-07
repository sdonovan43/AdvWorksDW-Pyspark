"""
Data Extraction Module
Extracts raw data from AdventureWorksDW2022 database
"""

import pandas as pd
from typing import Dict, List
from utils.db_utils import DatabaseConnector

class DataExtractor:
    """Class for extracting data from SQL Server database"""
    
    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector
    
    def extract_table(self, table_name: str) -> pd.DataFrame:
        """Extract a single table from the database"""
        query = f"SELECT * FROM {table_name}"
        print(f"Extracting data from {table_name}...")
        return self.db.execute_query(query)
    
    def extract_all_tables(self, table_names: List[str] = None) -> Dict[str, pd.DataFrame]:
        """Extract multiple tables from the database"""
        dataframes = {}
        
        if table_names is None:
            # Get all tables
            table_names = self.db.get_all_table_names()
        
        for table_name in table_names:
            try:
                df = self.extract_table(table_name)
                dataframes[table_name] = df
                print(f"Successfully extracted {len(df)} rows from {table_name}")
            except Exception as e:
                print(f"Error extracting {table_name}: {e}")
                continue
        
        return dataframes
    
    def save_raw_data(self, dataframes: Dict[str, pd.DataFrame], output_dir: str = "data/raw"):
        """Save extracted data to CSV files"""
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        for table_name, df in dataframes.items():
            file_path = f"{output_dir}/{table_name}.csv"
            df.to_csv(file_path, index=False)
            print(f"Saved {table_name} to {file_path}")

# Example usage:
if __name__ == "__main__":
    # Initialize database connector
    db = DatabaseConnector()
    extractor = DataExtractor(db)
    
    # Extract key tables for our analytics lab
    key_tables = [
        'DimCustomer',
        'DimProduct',
        'DimDate',
        'DimGeography',
        'FactInternetSales'
    ]
    
    # Extract data
    raw_data = extractor.extract_all_tables(key_tables)
    
    # Save raw data
    extractor.save_raw_data(raw_data)
    
    print("Extraction complete!")