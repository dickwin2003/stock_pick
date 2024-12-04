import baostock as bs
import pandas as pd
import pymysql
from datetime import datetime, timedelta
import time
from config import DB_CONFIG, BAOSTOCK_CONFIG

# MySQL connection configuration
def get_db_connection():
    return pymysql.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database']
    )

def create_kline_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Drop table if exists and create new one
    drop_table_sql = "DROP TABLE IF EXISTS stock_kline"
    cursor.execute(drop_table_sql)
    
    # Create table for K-line data
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stock_kline (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(20),
        date DATE,
        open DECIMAL(10,2),
        high DECIMAL(10,2),
        low DECIMAL(10,2),
        close DECIMAL(10,2),
        volume BIGINT,
        amount DECIMAL(16,2),
        adjustflag TINYINT,
        turn DECIMAL(10,2),
        tradestatus TINYINT,
        pctChg DECIMAL(10,2),
        peTTM DECIMAL(10,2),
        pbMRQ DECIMAL(10,2),
        psTTM DECIMAL(10,2),
        pcfNcfTTM DECIMAL(10,2),
        update_time DATETIME,
        INDEX idx_code_date (code, date)
    )
    """
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()

def get_stock_codes():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT code FROM stock_codes WHERE trade_status = '1'")
    codes = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return codes

def get_k_data(code, start_date, end_date):
    rs = bs.query_history_k_data_plus(code,
        "date,code,open,high,low,close,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="3")
    
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    return pd.DataFrame(data_list, columns=rs.fields)

def convert_to_float(value):
    try:
        return float(value) if value.strip() else 0.0
    except (ValueError, AttributeError):
        return 0.0

def insert_k_data(data):
    if data.empty:
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    current_time = datetime.now()
    for _, row in data.iterrows():
        sql = """
        INSERT INTO stock_kline (
            code, date, open, high, low, close, volume, amount,
            adjustflag, turn, tradestatus, pctChg, peTTM, pbMRQ,
            psTTM, pcfNcfTTM, update_time
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        """
        values = (
            row['code'], row['date'],
            convert_to_float(row['open']), convert_to_float(row['high']),
            convert_to_float(row['low']), convert_to_float(row['close']),
            int(float(row['volume'])) if row['volume'].strip() else 0,
            convert_to_float(row['amount']),
            int(float(row['adjustflag'])) if row['adjustflag'].strip() else 0,
            convert_to_float(row['turn']),
            int(float(row['tradestatus'])) if row['tradestatus'].strip() else 0,
            convert_to_float(row['pctChg']),
            convert_to_float(row['peTTM']),
            convert_to_float(row['pbMRQ']),
            convert_to_float(row['psTTM']),
            convert_to_float(row['pcfNcfTTM']),
            current_time
        )
        cursor.execute(sql, values)
    
    conn.commit()
    cursor.close()
    conn.close()

def main():
    # 登录系统
    lg = bs.login()
    print('login respond error_code:' + lg.error_code)
    print('login respond  error_msg:' + lg.error_msg)
    
    try:
        # 创建K线数据表
        create_kline_table()
        
        # 获取所有股票代码
        stock_codes = get_stock_codes()
        print(f"Found {len(stock_codes)} active stocks")
        
        # 设置时间范围（最近一年的数据）
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        # 获取并存储每支股票的K线数据
        for i, code in enumerate(stock_codes, 1):
            try:
                print(f"Processing {i}/{len(stock_codes)}: {code}")
                data = get_k_data(code, start_date, end_date)
                insert_k_data(data)
                # 避免请求过快
                time.sleep(BAOSTOCK_CONFIG['delay_seconds'])  # Rate limiting
            except Exception as e:
                print(f"Error processing {code}: {str(e)}")
                continue
            
        print("Successfully stored all K-line data in MySQL database")
    
    finally:
        # 退出系统
        bs.logout()

if __name__ == "__main__":
    main()