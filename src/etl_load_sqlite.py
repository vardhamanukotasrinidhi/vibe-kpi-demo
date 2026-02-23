import pandas as pd
import sqlite3
import os
import sys
from pathlib import Path

# Add config path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import MAX_MONTHLY_SPEND, CUSTOMERS_CSV_PATH, ANALYTICS_DB_PATH

def load_csv_to_sqlite():
    """Load CSV data to SQLite with validation and error handling"""
    try:
        # Ensure directories exist
        ANALYTICS_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        # Read CSV file with validation
        if not CUSTOMERS_CSV_PATH.exists():
            raise FileNotFoundError(f"Source CSV file not found: {CUSTOMERS_CSV_PATH}")
            
        df = pd.read_csv(str(CUSTOMERS_CSV_PATH))
        
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
            
        if (df['monthly_spend'] > MAX_MONTHLY_SPEND).any():
            raise ValueError(f"Monthly spend cannot exceed {MAX_MONTHLY_SPEND}")
            
        if not df['churned'].isin([0, 1]).all():
            raise ValueError("Churned values must be 0 or 1")
        
        # Create SQLite connection with context manager
        with sqlite3.connect(str(ANALYTICS_DB_PATH)) as conn:
            df.to_sql('customers_raw', conn, if_exists='replace', index=False)
        
        print(f"Successfully loaded {len(df)} validated rows into {ANALYTICS_DB_PATH}")
        
    except Exception as e:
        print(f"Error loading data: {e}")
        raise

if __name__ == "__main__":
    load_csv_to_sqlite()
