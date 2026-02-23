import pandas as pd
import sqlite3
import os

def load_csv_to_sqlite():
    """Load CSV data to SQLite with validation and error handling"""
    try:
        # Ensure directories exist
        os.makedirs('data/db', exist_ok=True)
        
        # Read CSV file with validation
        if not os.path.exists('data/raw/customers_raw.csv'):
            raise FileNotFoundError("Source CSV file not found")
            
        df = pd.read_csv('data/raw/customers_raw.csv')
        
        # Data validation
        if df.empty:
            raise ValueError("CSV file is empty")
            
        # Validate required columns
        required_cols = ['customer_id', 'city', 'monthly_spend', 'churned']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
            
        # Validate data types and ranges
        if (df['monthly_spend'] < 0).any():
            raise ValueError("Monthly spend cannot be negative")
            
        if not df['churned'].isin([0, 1]).all():
            raise ValueError("Churned values must be 0 or 1")
        
        # Create SQLite connection with context manager
        with sqlite3.connect('data/db/analytics.db') as conn:
            df.to_sql('customers_raw', conn, if_exists='replace', index=False)
        
        print(f"Successfully loaded {len(df)} validated rows into analytics.db")
        
    except Exception as e:
        print(f"Error loading data: {e}")
        raise

if __name__ == "__main__":
    load_csv_to_sqlite()
