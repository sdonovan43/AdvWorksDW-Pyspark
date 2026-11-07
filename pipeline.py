"""
Main ETL Pipeline for AdventureWorksDW2022 Data Engineering Lab
"""

import sys
import os
from src.extract import DataExtractor
from src.transform import DataTransformer
from src.load import DataLoader
from utils.db_utils import DatabaseConnector

def main():
    """Main ETL pipeline execution"""
    print("Starting AdventureWorksDW2022 ETL Pipeline...")
    
    # Initialize components
    db = DatabaseConnector()
    extractor = DataExtractor(db)
    transformer = DataTransformer()
    loader = DataLoader()
    
    try:
        # Step 1: Extract data from SQL Server
        print("\n1. Extracting raw data from AdventureWorksDW2022...")
        key_tables = [
            'DimCustomer',
            'DimProduct', 
            'DimDate',
            'DimGeography',
            'FactInternetSales'
        ]
        
        raw_data = extractor.extract_all_tables(key_tables)
        print(f"Extracted data from {len(raw_data)} tables")
        
        # Step 2: Transform data
        print("\n2. Transforming data...")
        transformed_data = transformer.transform_all(raw_data)
        print(f"Transformed data into {len(transformed_data)} tables")
        
        # Step 3: Load to Parquet files
        print("\n3. Loading data to Parquet files...")
        saved_files = loader.save_to_parquet(transformed_data, version="v1")
        print(f"Saved {len(saved_files)} Parquet files")
        
        # Step 4: Show file information
        print("\n4. Parquet file details:")
        for file_path in saved_files:
            if os.path.exists(file_path):
                info = loader.get_file_info(file_path)
                print(f"  {os.path.basename(file_path)}: "
                      f"{info.get('num_rows', 'N/A')} rows, "
                      f"{info.get('file_size', 'N/A')} bytes")
        
        print("\nPipeline execution completed successfully!")
        
    except Exception as e:
        print(f"Error in pipeline execution: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()