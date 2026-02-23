import sqlite3

def city_kpi(city: str) -> dict:
    """Calculate KPI for a specific city using parameterized SQL"""
    if not city or not isinstance(city, str):
        raise ValueError("City name must be a non-empty string")
    
    if len(city.strip()) == 0:
        raise ValueError("City name cannot be empty or whitespace")
    
    try:
        with sqlite3.connect('data/db/analytics.db') as conn:
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
                
                print(f"KPI for {city.strip()}:")
                print(f"  Total Customers: {total}")
                print(f"  Average Monthly Spend: ${avg_spend:.2f}")
                print(f"  Churned Customers: {churned}")
                print(f"  Churn Rate: {churn_rate}%")
                
                return kpi_data
            else:
                print(f"No data found for city: {city}")
                return {'city': city.strip(), 'error': 'No data found'}
                
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return {'city': city.strip(), 'error': f'Database error: {e}'}

if __name__ == "__main__":
    # Normal call
    city_kpi("Mumbai")
    
    # SQL injection attempt (should not return all rows)
    city_kpi("Mumbai' OR 1=1 --")
