"""
Data Loading Module
Loads transformed data to Parquet files (simulating Delta Lake storage)
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Dict, List
import os
from pathlib import Path

class DataLoader:
    """Class for loading data to Parquet files"""
    
    def __init__(self, output_dir: str = "data/processed"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
    def save_to_parquet(self, dataframes: Dict[str, pd.DataFrame], 
                       version: str = "v1") -> List[str]:
        """Save DataFrames to Parquet files with versioning"""
        saved_files = []
        
        for table_name, df in dataframes.items():
            # Create versioned filename
            filename = f"{table_name}_{version}.parquet"
            file_path = os.path.join(self.output_dir, filename)
            
            try:
                # Convert to PyArrow Table and save as Parquet
                table = pa.Table.from_pandas(df)
                pq.write_table(table, file_path)
                
                saved_files.append(file_path)
                print(f"Saved {table_name} to {file_path}")
                
            except Exception as e:
                print(f"Error saving {table_name}: {e}")
                continue
                
        return saved_files
    
    def save_with_version_control(self, dataframes: Dict[str, pd.DataFrame], 
                                table_name: str, version: str = "v1") -> str:
        """Save a single DataFrame with version control"""
        filename = f"{table_name}_{version}.parquet"
        file_path = os.path.join(self.output_dir, filename)
        
        # Create directory if needed
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        
        try:
            table = pa.Table.from_pandas(dataframes[table_name])
            pq.write_table(table, file_path)
            print(f"Saved {table_name} version {version} to {file_path}")
            return file_path
        except Exception as e:
            print(f"Error saving {table_name}: {e}")
            return ""
    
    def get_file_info(self, file_path: str) -> dict:
        """Get metadata about a Parquet file"""
        try:
            metadata = pq.read_metadata(file_path)
            return {
                'file_size': os.path.getsize(file_path),
                'num_rows': metadata.num_rows,
                'num_columns': metadata.num_columns
            }
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return {}
    
    def create_versioned_dataset(self, dataframes: Dict[str, pd.DataFrame], 
                               base_name: str) -> List[str]:
        """Create a version-controlled dataset with multiple versions"""
        # Create version directories
        version_dir = os.path.join(self.output_dir, f"{base_name}_versions")
        os.makedirs(version_dir, exist_ok=True)
        
        saved_files = []
        
        for version_num, (table_name, df) in enumerate(dataframes.items(), 1):
            version = f"v{version_num:03d}"
            filename = f"{table_name}_{version}.parquet"
            file_path = os.path.join(version_dir, filename)
            
            try:
                table = pa.Table.from_pandas(df)
                pq.write_table(table, file_path)
                saved_files.append(file_path)
                print(f"Saved {table_name} version {version} to {file_path}")
                
            except Exception as e:
                print(f"Error saving {table_name}: {e}")
                continue
                
        return saved_files

# Example usage:
if __name__ == "__main__":
    print("Data loading module initialized")