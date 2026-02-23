import pytest
import sqlite3
import pandas as pd
import os
import sys
from io import StringIO
from unittest.mock import patch

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from kpi_city import city_kpi
from etl_load_sqlite import load_csv_to_sqlite

@pytest.fixture
def setup_test_db():
    """Setup test database with sample data"""
    os.makedirs('data/db', exist_ok=True)
    
    # Create test data
    test_data = {
        'customer_id': [1, 2, 3, 4, 5],
        'city': ['Mumbai', 'Mumbai', 'Delhi', 'Bangalore', ''],
        'monthly_spend': [1000.0, 2000.0, 1500.0, 1200.0, 800.0],
        'churned': [0, 1, 0, 1, 0]
    }
    df = pd.DataFrame(test_data)
    
    # Load into test database
    conn = sqlite3.connect('data/db/analytics.db')
    df.to_sql('customers_raw', conn, if_exists='replace', index=False)
    conn.close()
    
    yield
    
    # Cleanup
    if os.path.exists('data/db/analytics.db'):
        os.remove('data/db/analytics.db')

def test_city_kpi_empty_city(setup_test_db):
    """Test KPI function with empty city name"""
    with pytest.raises(ValueError, match="City name must be a non-empty string"):
        city_kpi("")

def test_city_kpi_whitespace_city(setup_test_db):
    """Test KPI function with whitespace-only city name"""
    with pytest.raises(ValueError, match="City name cannot be empty"):
        city_kpi("   ")

def test_city_kpi_non_string_input(setup_test_db):
    """Test KPI function with non-string input"""
    with pytest.raises(ValueError, match="City name must be a non-empty string"):
        city_kpi(123)

def test_city_kpi_none_input(setup_test_db):
    """Test KPI function with None input"""
    with pytest.raises(ValueError, match="City name must be a non-empty string"):
        city_kpi(None)

def test_city_kpi_nonexistent_city(setup_test_db):
    """Test KPI function with city that doesn't exist in data"""
    result = city_kpi("NonExistentCity")
    assert result['error'] == 'No data found'
    assert result['city'] == 'NonExistentCity'

def test_etl_load_missing_file():
    """Test ETL function when CSV file is missing"""
    # Rename existing file temporarily
    if os.path.exists('data/raw/customers_raw.csv'):
        os.rename('data/raw/customers_raw.csv', 'data/raw/customers_raw.csv.bak')
    
    try:
        with pytest.raises(FileNotFoundError, match="Source CSV file not found"):
            load_csv_to_sqlite()
    finally:
        # Restore file
        if os.path.exists('data/raw/customers_raw.csv.bak'):
            os.rename('data/raw/customers_raw.csv.bak', 'data/raw/customers_raw.csv')

def test_etl_load_invalid_data():
    """Test ETL function with invalid data (negative spend)"""
    # Create invalid test data
    invalid_data = {
        'customer_id': [1, 2],
        'city': ['Mumbai', 'Delhi'],
        'monthly_spend': [1000.0, -500.0],  # Negative spend
        'churned': [0, 1]
    }
    df = pd.DataFrame(invalid_data)
    
    # Backup original file and create invalid one
    if os.path.exists('data/raw/customers_raw.csv'):
        os.rename('data/raw/customers_raw.csv', 'data/raw/customers_raw.csv.bak')
    
    df.to_csv('data/raw/customers_raw.csv', index=False)
    
    try:
        with pytest.raises(ValueError, match="Monthly spend cannot be negative"):
            load_csv_to_sqlite()
    finally:
        # Restore original file
        if os.path.exists('data/raw/customers_raw.csv.bak'):
            os.rename('data/raw/customers_raw.csv.bak', 'data/raw/customers_raw.csv')
