# Contributing to AdventureWorksDW2022 Data Engineering Lab

This project implements a local data engineering and analytics lab that mirrors cloud-scale Microsoft Fabric architecture. All contributions are welcome!

## Project Structure

```
AdvWorksDW-Pyspark/
├── data/                    # Data storage (raw and processed)
│   ├── raw/                # Raw extracted data
│   └── processed/          # Processed Parquet files
├── notebooks/              # Jupyter notebooks and exploratory analysis
│   └── etl_pipelines/      # ETL pipeline demonstrations
├── src/                    # Source code modules
│   ├── extract.py          # Data extraction logic  
│   ├── transform.py        # Data transformation logic
│   └── load.py             # Data loading logic
├── utils/                  # Utility functions
│   └── db_utils.py         # Database connection utilities
├── pipeline.py             # Main ETL pipeline script
├── duckdb_integration.py   # DuckDB for advanced analytics
├── setup.py                # Setup and installation script
├── requirements.txt        # Python dependencies
└── README.md               # Project documentation
```

## Getting Started

### Prerequisites

- Local SQL Server instance with AdventureWorksDW2022 database
- Python 3.7+
- VS Code (recommended IDE)
- Git for version control

### Installation

1. Clone the repository:
```bash
git clone https://github.com/your-username/AdvWorksDW-Pyspark.git
```

2. Navigate to project directory:
```bash
cd AdvWorksDW-Pyspark
```

3. Install dependencies:
```bash
python setup.py
```

### Running the Pipeline

1. Execute the main pipeline:
```bash
python pipeline.py
```

2. Run DuckDB integration demo:
```bash
python duckdb_integration.py
```

## Development Workflow

### 1. Data Extraction
- Connects to AdventureWorksDW2022 database
- Extracts core dimensional and fact tables
- Saves raw data temporarily for reference

### 2. Data Transformation  
- Cleans and structures extracted data
- Applies dimensional modeling principles
- Creates analytics-friendly denormalized views
- Implements star schema design

### 3. Data Loading
- Converts processed data to Parquet format (Delta Lake style)
- Implements version control through filename conventions
- Supports partitioning and optimization

### 4. Advanced Analytics
- Uses DuckDB for fast analytical queries
- Demonstrates SCD Type 3 replacement patterns
- Shows merge operations and version tracking

## Key Features Implemented

### Local Scaling Architecture
- Fully functional offline environment
- Mirrors cloud-scale Microsoft Fabric patterns
- No cloud licensing requirements
- Real-world data preparation practices

### Modern Data Engineering Patterns
- **ETL Pipeline**: Extract, Transform, Load automation  
- **Delta Lake Style Storage**: Parquet files with versioning
- **SCD Replacement**: DuckDB for advanced merge operations
- **Star Schema Design**: Proper dimensional modeling
- **Version Control**: Git integration for all transformations

### Technical Implementation Details
  
#### Data Extraction (`src/extract.py`)
- Uses pyodbc and SQL Server connector  
- Extracts core tables from AdventureWorksDW2022
- Handles connection management and error recovery

#### Data Transformation (`src/transform.py`) 
- Implements dimensional modeling best practices
- Creates derived columns and data cleansing logic
- Joins fact tables with dimension tables
- Builds summary metrics for analytics

#### Data Loading (`src/load.py`)
- Writes processed data as Parquet files 
- Implements version control patterns
- Supports delta-style updates (simulated)

#### Advanced Analytics (`duckdb_integration.py`)
- Demonstrates SCD Type 3 replacement using DuckDB merge operations
- Shows how to handle slowly changing dimensions
- Implements version tracking in analytical layer

## Version Control Best Practices

### Commit Strategy
1. **Feature Branches**: Create branches for new data transformations
2. **Atomic Commits**: Small, focused commits with descriptive messages
3. **Version Tags**: Tag releases with semantic versioning (v1.0.0, v1.1.0)
4. **Branch Protection**: Protect main and develop branches

### Data Versioning
- Each Parquet file includes version in filename (e.g., `FactInternetSales_v001.parquet`)
- Metadata tracking through DuckDB's schema evolution
- Change data capture for incremental processing

## Repository Standards

### Code Style
- Follow PEP 8 coding standards  
- Use descriptive variable and function names
- Include docstrings for all functions
- Type hints where appropriate

### Documentation
- Keep README updated with new features
- Add inline comments for complex logic
- Document data dictionaries in `data/` directory
- Update CONTRIBUTING.md for process improvements

## Troubleshooting

### Common Issues

1. **Connection Problems**: Ensure SQL Server instance `maingear` is running
2. **Missing Dependencies**: Run `python setup.py` to install requirements  
3. **Permission Errors**: Run as administrator if needed
4. **File Access Errors**: Check directory permissions for data folders

### Support

For issues or questions, please:
1. Check the existing issues in GitHub repository
2. Create a new issue with detailed description
3. Include relevant error messages and steps to reproduce

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Microsoft AdventureWorksDW2022 sample database
- Community resources for data engineering patterns
- Open source tools (DuckDB, PyArrow, SQL Server)