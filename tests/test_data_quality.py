import pytest
import sqlite3
import pandas as pd
import os
import sys

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl_load_sqlite import load_csv_to_sqlite

@pytest.fixture
def setup_quality_test():
    """Setup for data quality tests"""
    os.makedirs('data/db', exist_ok=True)
    os.makedirs('data/raw', exist_ok=True)
    
    # Backup original file
    if os.path.exists('data/raw/customers_raw.csv'):
        os.rename('data/raw/customers_raw.csv', 'data/raw/customers_raw.csv.bak')
    
    yield
    
    # Cleanup and restore
    if os.path.exists('data/db/analytics.db'):
        os.remove('data/db/analytics.db')
    if os.path.exists('data/raw/customers_raw.csv'):
        os.remove('data/raw/customers_raw.csv')
    if os.path.exists('data/raw/customers_raw.csv.bak'):
        os.rename('data/raw/customers_raw.csv.bak', 'data/raw/customers_raw.csv')

def test_etl_handles_null_values(setup_quality_test):
    """Test ETL handles null/missing values correctly"""
    # Create data with null values (but valid churned values)
    test_data = {
        'customer_id': [1, 2, 3],
        'city': ['Mumbai', None, 'Delhi'],
        'monthly_spend': [1000.0, 2000.0, None],
        'churned': [0, 1, 0]  # Valid churned values
    }
    df = pd.DataFrame(test_data)
    df.to_csv('data/raw/customers_raw.csv', index=False)
    
    # Should load successfully (pandas handles nulls)
    load_csv_to_sqlite()
    
    # Verify data was loaded
    conn = sqlite3.connect('data/db/analytics.db')
    loaded_df = pd.read_sql('SELECT * FROM customers_raw', conn)
    conn.close()
    
    assert len(loaded_df) == 3
    assert pd.isna(loaded_df.loc[1, 'city'])
    assert pd.isna(loaded_df.loc[2, 'monthly_spend'])

def test_etl_duplicate_customer_ids(setup_quality_test):
    """Test ETL handles duplicate customer IDs"""
    # Create data with duplicate customer IDs
    test_data = {
        'customer_id': [1, 1, 2],  # Duplicate ID 1
        'city': ['Mumbai', 'Delhi', 'Bangalore'],
        'monthly_spend': [1000.0, 2000.0, 1500.0],
        'churned': [0, 1, 0]
    }
    df = pd.DataFrame(test_data)
    df.to_csv('data/raw/customers_raw.csv', index=False)
    
    # Should load successfully (no uniqueness constraint)
    load_csv_to_sqlite()
    
    # Verify all rows were loaded
    conn = sqlite3.connect('data/db/analytics.db')
    loaded_df = pd.read_sql('SELECT * FROM customers_raw', conn)
    conn.close()
    
    assert len(loaded_df) == 3
    assert loaded_df['customer_id'].tolist() == [1, 1, 2]

def test_etl_large_dataset_performance(setup_quality_test):
    """Test ETL performance with larger dataset"""
    # Create larger dataset (1000 rows)
    import random
    
    cities = ['Mumbai', 'Delhi', 'Bangalore', 'Chennai']
    test_data = {
        'customer_id': range(1, 1001),
        'city': [random.choice(cities) for _ in range(1000)],
        'monthly_spend': [round(random.uniform(500, 5000), 2) for _ in range(1000)],
        'churned': [random.choice([0, 1]) for _ in range(1000)]
    }
    df = pd.DataFrame(test_data)
    df.to_csv('data/raw/customers_raw.csv', index=False)
    
    # Should handle larger dataset efficiently
    import time
    start_time = time.time()
    load_csv_to_sqlite()
    end_time = time.time()
    
    # Should complete within reasonable time (less than 5 seconds)
    assert end_time - start_time < 5.0
    
    # Verify all data was loaded
    conn = sqlite3.connect('data/db/analytics.db')
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM customers_raw')
    count = cursor.fetchone()[0]
    conn.close()
    
    assert count == 1000
