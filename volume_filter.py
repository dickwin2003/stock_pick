import pymysql
import pandas as pd
from datetime import datetime, timedelta

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'stock_data',
    'charset': 'utf8mb4'
}

def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

def create_tables(conn):
    cursor = conn.cursor()
    
    # 创建stock_kline表
    create_kline_table = """
    CREATE TABLE IF NOT EXISTS stock_kline (
        id INT AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(20),
        date DATE,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume FLOAT,
        amount FLOAT,
        INDEX idx_code_date (code, date)
    )
    """
    cursor.execute(create_kline_table)
    
    # 创建filtered_stocks表
    create_filtered_table = """
    CREATE TABLE IF NOT EXISTS filtered_stocks (
        id INT AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(20),
        trigger_date DATE,
        avg_volume_3m FLOAT,
        spike_volume FLOAT,
        volume_ratio FLOAT,
        ma10 FLOAT,
        ma20 FLOAT,
        close_price FLOAT,
        update_time DATETIME
    )
    """
    cursor.execute(create_filtered_table)
    conn.commit()
    cursor.close()

def process_stock_data(conn):
    cursor = conn.cursor()
    
    # 清空filtered_stocks表
    cursor.execute("TRUNCATE TABLE filtered_stocks")
    conn.commit()

    # 获取所有股票代码
    cursor.execute("SELECT DISTINCT code FROM stock_kline")
    stock_codes = cursor.fetchall()

    for (code,) in stock_codes:
        try:
            # 获取最近6个月的数据
            sql = """
            SELECT date, volume, close 
            FROM stock_kline 
            WHERE code = %s 
            AND date >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
            ORDER BY date
            """
            cursor.execute(sql, (code,))
            rows = cursor.fetchall()
            
            if len(rows) < 60:  # 确保有足够的数据
                continue
                
            # 转换为DataFrame
            df = pd.DataFrame(rows, columns=['date', 'volume', 'close'])
            df.set_index('date', inplace=True)
            
            # 计算3个月平均成交量
            df['avg_volume_3m'] = df['volume'].rolling(window=60).mean()
            
            # 计算10日和20日均线
            df['ma10'] = df['close'].rolling(window=10).mean()
            df['ma20'] = df['close'].rolling(window=20).mean()
            
            # 获取最近10天的数据
            recent_data = df.tail(10)
            
            if len(recent_data) < 10:
                continue
            
            # 计算最近3个月（除去最近10天）的平均成交量
            past_3m_avg = df['volume'].iloc[:-10].tail(60).mean()
            
            # 检查是否之前处于横盘低量状态
            past_volume_std = df['volume'].iloc[:-10].tail(60).std() / past_3m_avg
            if past_volume_std > 0.8:  # 如果之前波动太大，说明不是横盘
                continue
                
            # 检查最近10天是否有3倍量
            volume_spikes = recent_data[recent_data['volume'] >= past_3m_avg * 3]
            
            if len(volume_spikes) > 0:
                # 检查10天内交易量是否持续增加
                recent_volumes = recent_data['volume'].tolist()
                volume_increase_count = sum(1 for i in range(len(recent_volumes)-1) if recent_volumes[i] < recent_volumes[i+1])
                
                if volume_increase_count >= 7:  # 至少8天是上升的
                    # 获取最新价格和均线
                    latest_data = df.iloc[-1]
                    latest_close = latest_data['close']
                    latest_ma10 = latest_data['ma10']
                    latest_ma20 = latest_data['ma20']
                    
                    # 检查价格是否在10-20日均线之间
                    if latest_ma10 >= latest_close >= latest_ma20:
                        # 获取第一个放量日期
                        spike_row = volume_spikes.iloc[0]
                        
                        # 插入数据
                        insert_sql = """
                        INSERT INTO filtered_stocks 
                        (code, trigger_date, avg_volume_3m, spike_volume, volume_ratio, ma10, ma20, close_price, update_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        
                        cursor.execute(insert_sql, (
                            code,
                            spike_row.name,
                            float(past_3m_avg),
                            float(spike_row['volume']),
                            float(spike_row['volume'] / past_3m_avg),
                            float(latest_ma10),
                            float(latest_ma20),
                            float(latest_close),
                            datetime.now()
                        ))
                        conn.commit()
            
            print(f"Processed {code}")
            
        except Exception as e:
            print(f"Error processing {code}: {str(e)}")
            continue
    
    cursor.close()

def filter_stocks():
    conn = get_db_connection()
    create_tables(conn)
    process_stock_data(conn)
    conn.close()

if __name__ == "__main__":
    filter_stocks()
