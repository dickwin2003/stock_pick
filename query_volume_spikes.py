import pymysql
import pandas as pd
from config import DB_CONFIG

def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

def query_significant_spikes(min_ratio=3.0, min_post_ratio=1.5):
    """
    Query volume spikes where:
    1. The spike day volume is at least min_ratio times the previous average
    2. The post-spike average volume is at least min_post_ratio times the pre-spike average
    """
    conn = get_db_connection()
    
    query = f"""
    SELECT 
        v.code,
        s.code_name,
        v.spike_date,
        v.pre_avg_amount,
        v.spike_amount,
        v.amount_ratio,
        v.post_avg_amount,
        v.post_amount_ratio,
        v.close_price
    FROM volume_spikes v
    JOIN stock_codes s ON v.code = s.code
    WHERE v.amount_ratio >= {min_ratio}
    AND v.post_amount_ratio >= {min_post_ratio}
    ORDER BY v.spike_date DESC, v.amount_ratio DESC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Format the amounts to be more readable (convert to millions)
    df['pre_avg_amount'] = df['pre_avg_amount'] / 1000000
    df['spike_amount'] = df['spike_amount'] / 1000000
    df['post_avg_amount'] = df['post_avg_amount'] / 1000000
    
    # Round the ratios
    df['amount_ratio'] = df['amount_ratio'].round(2)
    df['post_amount_ratio'] = df['post_amount_ratio'].round(2)
    
    print("\nSignificant Volume Spikes Analysis")
    print("==================================")
    print(f"Showing stocks with:")
    print(f"- Spike day volume >= {min_ratio}x previous average")
    print(f"- Post-spike average volume >= {min_post_ratio}x pre-spike average")
    print("\nResults:")
    print(df.to_string(index=False))
    
    # Save to CSV
    csv_file = "volume_spikes_analysis.csv"
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    print(f"\nResults saved to {csv_file}")

if __name__ == "__main__":
    query_significant_spikes()
