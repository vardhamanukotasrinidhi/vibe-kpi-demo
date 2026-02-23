import sqlite3
# Add at top
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def city_kpi(city: str) -> dict:
    """Calculate KPI for a specific city using parameterized SQL"""
    if not city or not isinstance(city, str):
        raise ValueError("City name must be a non-empty string")
    
    if len(city.strip()) == 0:
        raise ValueError("City name cannot be empty or whitespace")
    
    try:
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from config import ANALYTICS_DB_PATH
        
        with sqlite3.connect(str(ANALYTICS_DB_PATH)) as conn:
            cursor = conn.cursor()
            
            # Parameterized query to prevent SQL injection
            query = """
            SELECT 
                COUNT(*) as total_customers,
                COALESCE(AVG(monthly_spend), 0) as avg_spend,
                COALESCE(SUM(churned), 0) as churned_count,
                COALESCE(ROUND(AVG(churned) * 100, 2), 0) as churn_rate_pct
            FROM customers_raw 
            WHERE city = ?
            """
            
            cursor.execute(query, (city.strip(),))
            result = cursor.fetchone()
            
            if result and result[0] > 0:
                total, avg_spend, churned, churn_rate = result
                kpi_data = {
                    'city': city.strip(),
                    'total_customers': total,
                    'avg_monthly_spend': float(avg_spend),
                    'churned_customers': churned,
                    'churn_rate_pct': float(churn_rate)
                }
                
                logger.info(f"KPI for {city.strip()}:")
                logger.info(f"  Total Customers: {total}")
                logger.info(f"  Average Monthly Spend: ${avg_spend:.2f}")
                logger.info(f"  Churned Customers: {churned}")
                logger.info(f"  Churn Rate: {churn_rate}%")
                
                return kpi_data
            else:
                logger.info(f"No data found for city: {city}")
                return create_error_response(city, 'No data found')
                
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return create_error_response(city, f'Database error: {e}')

def create_error_response(city: str, error_msg: str) -> dict:
    """Create standardized error response"""
    return {'city': city.strip(), 'error': error_msg, 'success': False}

if __name__ == "__main__":
    # Normal call
    city_kpi("Mumbai")
    
    # SQL injection attempt (should not return all rows)
    city_kpi("Mumbai' OR 1=1 --")
