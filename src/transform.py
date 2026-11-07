"""
Data Transformation Module
Transforms raw data into analytical formats for Parquet storage
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import os

class DataTransformer:
    """Class for transforming raw data into analytical formats"""
    
    def __init__(self):
        pass
    
    def clean_customer_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and transform customer data"""
        # Create derived columns
        df['FullName'] = df['FirstName'] + ' ' + df['LastName']
        df['Age'] = 2023 - df['BirthDate'].dt.year
        
        # Handle missing values
        df['MiddleName'] = df['MiddleName'].fillna('')
        df['Suffix'] = df['Suffix'].fillna('')
        
        # Rename columns for better consistency
        df = df.rename(columns={
            'CustomerAlternateKey': 'CustomerID',
            'YearlyIncome': 'AnnualIncome'
        })
        
        return df
    
    def clean_product_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and transform product data"""
        # Create derived columns
        df['ProductLine'] = df['ProductLine'].fillna('N/A')
        df['Size'] = df['Size'].fillna('Unknown')
        
        # Rename columns for consistency 
        df = df.rename(columns={
            'ProductAlternateKey': 'ProductID',
            'StandardCost': 'Cost'
        })
        
        return df
    
    def clean_date_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and transform date data"""
        # Ensure date columns are properly parsed
        df['FullDate'] = pd.to_datetime(df['FullDate'], errors='coerce')
        df['MonthNumberOfYear'] = df['MonthNumberOfYear'].astype(int)
        
        return df
    
    def transform_sales_data(self, sales_df: pd.DataFrame, dim_date: pd.DataFrame, 
                           dim_customer: pd.DataFrame) -> pd.DataFrame:
        """Transform sales data by joining with dimension tables"""
        # Convert date keys to datetime for analysis
        date_map = dict(zip(dim_date['DateKey'], dim_date['FullDate']))
        sales_df['OrderDate'] = sales_df['OrderDateKey'].map(date_map)
        
        # Join with customer data to add customer demographics
        customer_map = dict(zip(dim_customer['CustomerKey'], 
                               dim_customer['FullName']))
        sales_df['CustomerName'] = sales_df['CustomerKey'].map(customer_map)
        
        return sales_df
    
    def create_summary_metrics(self, sales_data: pd.DataFrame) -> pd.DataFrame:
        """Create summary metrics for analytics"""
        # Group by year and month
        sales_data['OrderYear'] = sales_data['OrderDate'].dt.year
        sales_data['OrderMonth'] = sales_data['OrderDate'].dt.month
        
        # Create summary statistics
        summary_df = sales_data.groupby(['OrderYear', 'OrderMonth']).agg({
            'SalesAmount': ['sum', 'count', 'mean'],
            'OrderQuantity': 'sum',
            'TotalProductCost': 'sum'
        }).round(2)
        
        summary_df.columns = ['TotalSales', 'TransactionCount', 'AvgSaleAmount', 
                             'TotalQuantitySold', 'TotalCost']
        
        summary_df = summary_df.reset_index()
        
        return summary_df
    
    def transform_all(self, raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Apply all transformations to raw data"""
        transformed_data = {}
        
        # Transform individual tables
        if 'DimCustomer' in raw_data:
            transformed_data['DimCustomer'] = self.clean_customer_data(raw_data['DimCustomer'])
            
        if 'DimProduct' in raw_data:
            transformed_data['DimProduct'] = self.clean_product_data(raw_data['DimProduct'])
            
        if 'DimDate' in raw_data:
            transformed_data['DimDate'] = self.clean_date_data(raw_data['DimDate'])
            
        if 'FactInternetSales' in raw_data:
            # Transform sales data with dimension joins
            sales_data = self.transform_sales_data(
                raw_data['FactInternetSales'],
                raw_data.get('DimDate', pd.DataFrame()),
                raw_data.get('DimCustomer', pd.DataFrame())
            )
            transformed_data['FactInternetSales'] = sales_data
            
        # Create summary metrics
        if 'FactInternetSales' in transformed_data and 'DimDate' in transformed_data:
            summary_metrics = self.create_summary_metrics(transformed_data['FactInternetSales'])
            transformed_data['SalesSummary'] = summary_metrics
            
        return transformed_data

# Example usage:
if __name__ == "__main__":
    # This would typically be called from a main pipeline script
    print("Data transformation module initialized")