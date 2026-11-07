# PySpark Implementation for AdventureWorksDW2022 Data Engineering Lab

This directory contains the PySpark implementation of the AdventureWorksDW2022 Data Engineering Lab, optimized for Databricks Community Edition.

## Overview

A complete distributed ETL pipeline that demonstrates enterprise-scale data engineering using Apache Spark. This implementation processes AdventureWorksDW2022 data with:

- **Extract**: JDBC connectivity to SQL Server AdventureWorksDW2022 database  
- **Transform**: Spark DataFrames for parallel data processing
- **Load**: Databricks File System (DBFS) optimized storage

## Repository Structure

```
spark_version/
├── final_databricks_pipeline.py  # Main PySpark ETL pipeline
├── spark_setup.py                # Spark session configuration
└── README.md                     # This documentation
```

## Features

1. **Distributed Processing** - Leverages Spark's parallel computing capabilities  
2. **SQL Server Integration** - Direct JDBC connection to AdventureWorksDW2022 database
3. **DBFS Optimization** - Efficient storage in Databricks environment  
4. **Spark SQL** - Query optimization with DataFrame API
5. **Scalable Architecture** - Enterprise-ready data processing patterns

## Prerequisites

- Databricks Community Edition account (free)
- Apache Spark 3.x or higher
- Python 3.7+
- AdventureWorksDW2022 database accessible via SQL Server

## Installation

```bash
# In Databricks notebook, install dependencies:
# %pip install pyspark pyodbc
```

## Usage

### In Databricks:
1. Copy `final_databricks_pipeline.py` into a Databricks notebook  
2. Update database connection credentials in `get_database_credentials()`
3. Run all cells to execute the ETL pipeline

### Command Line (if running outside Databricks):
```bash
# Update your Spark configuration and run:
python final_databricks_pipeline.py
```

## Learning Value for DP-600

This implementation covers key topics:
- ✅ Distributed computing patterns with Spark  
- ✅ Enterprise-scale data engineering
- ✅ Spark SQL and DataFrame operations
- ✅ Cloud-native data processing
- ✅ JDBC connectivity in distributed environments  
- ✅ Performance optimization techniques

## Repository Status

**Version 1.0.0 - v0.1**

This is a complete, functional PySpark implementation that demonstrates enterprise-scale data engineering using Apache Spark.