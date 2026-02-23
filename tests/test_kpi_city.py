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

@pytest.fixture
def setup_test_db():
    """Setup test database with sample data"""
    # Ensure test directory exists
    os.makedirs('data/db', exist_ok=True)
    
    # Create test data
    test_data = {
        'customer_id': [1, 2, 3, 4],
        'city': ['Mumbai', 'Mumbai', 'Delhi', 'Bangalore'],
        'monthly_spend': [1000.0, 2000.0, 1500.0, 1200.0],
        'churned': [0, 1, 0, 1]
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

def test_city_kpi_happy_path(setup_test_db):
    """Test normal city KPI calculation"""
    with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
        city_kpi("Mumbai")
        output = mock_stdout.getvalue()
    
    assert "KPI for Mumbai:" in output
    assert "Total Customers: 2" in output
    assert "Average Monthly Spend: $1500.00" in output
    assert "Churned Customers: 1" in output
    assert "Churn Rate: 50.0%" in output

def test_city_kpi_injection_attempt(setup_test_db):
    """Test SQL injection attempt should not return all rows"""
    with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
        city_kpi("Mumbai' OR 1=1 --")
        output = mock_stdout.getvalue()
    
    # Should not find data for this injection attempt
    assert "No data found for city: Mumbai' OR 1=1 --" in output
    # Should not show KPI data
    assert "Total Customers:" not in output
