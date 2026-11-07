"""
Setup script for AdventureWorksDW2022 Data Engineering Lab
"""

import subprocess
import sys
import os

def install_requirements():
    """Install required Python packages"""
    print("Installing required packages...")
    
    try:
        # Install from requirements.txt
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("Packages installed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error installing packages: {e}")
        return False

def check_sql_server_connection():
    """Check if we can connect to SQL Server"""
    try:
        import pyodbc
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=maingear;"
            "DATABASE=AdventureWorksDW2022;"
            "Trusted_Connection=yes;"
        )
        conn.close()
        print("✓ Successfully connected to SQL Server (maingear)")
        return True
    except Exception as e:
        print(f"✗ Failed to connect to SQL Server: {e}")
        return False

def create_project_structure():
    """Create necessary project directories"""
    directories = [
        "data/raw",
        "data/processed", 
        "notebooks/etl_pipelines",
        "src",
        "utils"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✓ Created directory: {directory}")

def main():
    """Main setup function"""
    print("AdventureWorksDW2022 Data Engineering Lab Setup")
    print("=" * 50)
    
    # Create project structure
    print("\n1. Creating project structure...")
    create_project_structure()
    
    # Check SQL Server connection
    print("\n2. Checking SQL Server connection...")
    sql_connected = check_sql_server_connection()
    
    # Install requirements
    print("\n3. Installing Python dependencies...")
    packages_installed = install_requirements()
    
    if sql_connected and packages_installed:
        print("\n✓ Setup completed successfully!")
        print("\nNext steps:")
        print("1. Run the pipeline: python pipeline.py")
        print("2. Explore the notebooks in /notebooks/")
        print("3. Check the data in /data/processed/")
        print("4. Use DuckDB for advanced analytics: python duckdb_integration.py")
    else:
        print("\n⚠ Setup completed with some issues - please check above errors")

if __name__ == "__main__":
    main()