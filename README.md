# AdventureWorksDW2022 Data Engineering Lab

This project implements a local data engineering and analytics lab that mirrors a cloud-scale Microsoft Fabric architecture. Using VS Code, SQL Server, SSMS, and Power BI Desktop, we extract raw tables from the AdventureWorksDW2022 database, process them into analytical dataframes, and write them out as Parquet files to mimic a modern delta-lake storage layer.

## Architecture Overview

This lab demonstrates enterprise-grade analytics patterns using:
- Local SQL Server with AdventureWorksDW2022 database
- Python ETL pipelines for data processing
- Parquet file formats for storage (simulating Delta Lake)
- DuckDB for advanced analytics and versioning
- GitHub for version control

## Project Structure

```
AdvWorksDW-Pyspark/
├── data/
│   ├── raw/           # Raw extracted data from SQL Server
│   └── processed/     # Processed Parquet files
├── notebooks/
│   └── etl_pipelines/ # ETL scripts and notebooks
├── src/
│   ├── extract.py     # Data extraction from SQL Server
│   ├── transform.py   # Data transformation logic
│   └── load.py        # Data loading to Parquet files
├── utils/
│   └── db_utils.py    # Database utilities
└── requirements.txt   # Python dependencies
```

## Key Features

1. **Modern ETL Pipeline**: Extract, Transform, Load using Python and pandas
2. **Delta-Lake Style Storage**: Using Parquet files with version tracking
3. **SCD Type 3 Replacement**: Using DuckDB for advanced merge operations
4. **Star Schema Design**: Properly normalized dimensional models
5. **GitHub Integration**: Version control for all transformations

## Getting Started

1. Clone the repository
2. Run `python setup.py` to install dependencies  
3. Execute `python pipeline.py` to run the full ETL pipeline
4. Explore results in `/data/processed/`

## Version History

**v0.1** - Initial release
- Complete ETL pipeline implementation
- Support for AdventureWorksDW2022 database
- Parquet file storage with version control
- DuckDB integration for advanced analytics