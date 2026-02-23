"""Configuration settings for the analytics project"""

import os
from pathlib import Path

# Base directory
BASE_DIR = Path(__file__).parent

# Data paths
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
DB_DIR = DATA_DIR / "db"

# File paths
CUSTOMERS_CSV_PATH = RAW_DATA_DIR / "customers_raw.csv"
ANALYTICS_DB_PATH = DB_DIR / "analytics.db"

# Database settings
DB_TIMEOUT = 30.0

# Data validation settings
MIN_MONTHLY_SPEND = 0.0
MAX_MONTHLY_SPEND = 100000.0
VALID_CHURN_VALUES = [0, 1]

# Ensure directories exist
def ensure_directories():
    """Create necessary directories if they don't exist"""
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    DB_DIR.mkdir(parents=True, exist_ok=True)
