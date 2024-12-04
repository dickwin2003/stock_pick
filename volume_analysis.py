import pymysql
import pandas as pd
from datetime import datetime
from config import DB_CONFIG

def get_db_connection():
    return pymysql.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database']
    )

def create_volume_spike_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Create table for volume spikes
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS volume_spikes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(10) NOT NULL,
        spike_date DATE NOT NULL,
        pre_avg_amount DECIMAL(20,2),
        spike_amount DECIMAL(20,2),
        amount_ratio DECIMAL(10,2),
        post_avg_amount DECIMAL(20,2),
        post_amount_ratio DECIMAL(10,2),
        close_price DECIMAL(10,2),
        update_time DATETIME,
        INDEX idx_code_date (code, spike_date)
    )
    """
    cursor.execute(create_table_sql)
    conn.commit()
    conn.close()

def analyze_volume_spikes(lookback_days=10, post_days=5, volume_threshold=3.0):
    conn = get_db_connection()
    
    # Get all stock codes
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT code FROM stock_kline")
    stock_codes = [row[0] for row in cursor.fetchall()]
    
    # Create volume_spikes table if not exists
    create_volume_spike_table()
    
    for code in stock_codes:
        try:
            # Get k-line data for this stock
            query = f"""
            SELECT date, amount, close 
            FROM stock_kline 
            WHERE code = '{code}'
            ORDER BY date
            """
            df = pd.read_sql(query, conn)
            
            if len(df) < lookback_days + post_days:
                continue
                
            # Calculate rolling average volume
            df['rolling_avg_amount'] = df['amount'].rolling(window=lookback_days, min_periods=lookback_days).mean()
            
            # Find volume spikes
            df['amount_ratio'] = df['amount'] / df['rolling_avg_amount']
            
            # Get dates where volume is 3x the average
            spike_dates = df[df['amount_ratio'] >= volume_threshold].index
            
            for spike_idx in spike_dates:
                if spike_idx + post_days >= len(df):
                    continue
                    
                spike_row = df.iloc[spike_idx]
                
                # Calculate average volume after spike
                post_period = df.iloc[spike_idx + 1:spike_idx + post_days + 1]
                pre_period = df.iloc[max(0, spike_idx - lookback_days):spike_idx]
                
                post_avg_amount = post_period['amount'].mean()
                pre_avg_amount = pre_period['amount'].mean()
                
                # Only consider if post-spike volume remains higher than pre-spike
                if post_avg_amount > pre_avg_amount:
                    # Insert into volume_spikes table
                    insert_sql = """
                    INSERT INTO volume_spikes 
                    (code, spike_date, pre_avg_amount, spike_amount, amount_ratio,
                     post_avg_amount, post_amount_ratio, close_price, update_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    cursor.execute(insert_sql, (
                        code,
                        df.iloc[spike_idx]['date'],
                        float(pre_avg_amount),
                        float(spike_row['amount']),
                        float(spike_row['amount_ratio']),
                        float(post_avg_amount),
                        float(post_avg_amount / pre_avg_amount),
                        float(spike_row['close']),
                        datetime.now()
                    ))
                    conn.commit()
            
            print(f"Processed {code}")
            
        except Exception as e:
            print(f"Error processing {code}: {str(e)}")
            continue
    
    conn.close()

if __name__ == "__main__":
    analyze_volume_spikes()
