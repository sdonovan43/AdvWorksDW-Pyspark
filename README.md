# AdventureWorksDW-Pyspark

PySpark implementation of the AdventureWorksDW2022 Data Engineering Lab for Databricks Community Edition.

## Overview

Complete distributed ETL pipeline using Apache Spark and PySpark to process AdventureWorksDW2022 data in a cloud-scale environment.

## Repository Structure

```
AdvWorksDW-Pyspark/
├── spark_version/           # PySpark implementation
│   ├── final_databricks_pipeline.py  # Main ETL pipeline  
│   ├── spark_setup.py                # Spark session configuration
│   └── README.md                     # Documentation for PySpark version
├── requirements.txt         # Spark dependencies
└── README.md                # This file
```

## Features

- Distributed processing with Spark DataFrames  
- SQL Server JDBC connectivity
- Databricks File System (DBFS) storage
- Enterprise-scale data engineering patterns
- Ready for Databricks Community Edition

## Getting Started

1. Clone repository
2. Install dependencies: `pip install -r requirements.txt`
3. Copy `spark_version/final_databricks_pipeline.py` to Databricks notebook
4. Update database credentials
5. Run ETL pipeline 

## Learning Value for DP-600

This implementation covers:
- ✅ Distributed computing patterns with Spark
- ✅ Enterprise-scale data engineering  
- ✅ Spark SQL and DataFrame operations
- ✅ Cloud-native data processing
- ✅ JDBC connectivity in distributed environments

## Version

**v0.1** - Initial release