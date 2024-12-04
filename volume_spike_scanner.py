import baostock as bs
import pandas as pd
import pymysql
from config import DB_CONFIG
import datetime
import time

def connect_database():
    """连接到MySQL数据库"""
    return pymysql.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database']
    )

def get_stock_data(stock_code, start_date, end_date):
    """获取指定股票的历史数据"""
    rs = bs.query_history_k_data_plus(
        stock_code,
        "date,code,volume",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="3"
    )
    
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    
    df = pd.DataFrame(data_list, columns=['date', 'code', 'volume'])
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
    return df

def check_volume_conditions(df):
    """检查交易量是否满足条件"""
    if len(df) < 30:  # 确保有足够的数据
        return False
        
    # 获取最近3天的数据
    recent_days = df.iloc[-3:]
    # 获取之前的数据（3-30天）
    previous_days = df.iloc[-30:-3]
    
    # 条件1：3-30天内的交易量较少
    avg_volume_previous = previous_days['volume'].mean()
    
    # 条件2：最近1-3天内交易量是之前的3倍以上
    min_recent_volume = recent_days['volume'].min()
    if min_recent_volume < (avg_volume_previous * 3):
        return False
        
    # 条件3：最近1-3天的交易量保持增长
    recent_volumes = recent_days['volume'].values
    if not all(recent_volumes[i] <= recent_volumes[i+1] for i in range(len(recent_volumes)-1)):
        return False
        
    return True

def save_results(conn, stock_code, scan_date):
    """保存筛选结果到数据库"""
    cursor = conn.cursor()
    try:
        # 创建表（如果不存在）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS volume_spike_results (
                id INT AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(10),
                scan_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 插入数据
        sql = "INSERT INTO volume_spike_results (stock_code, scan_date) VALUES (%s, %s)"
        cursor.execute(sql, (stock_code, scan_date))
        conn.commit()
    except Exception as e:
        print(f"Error saving results for {stock_code}: {str(e)}")
    finally:
        cursor.close()

def main():
    # 登录系统
    bs.login()
    
    try:
        # 连接数据库
        conn = connect_database()
        
        # 获取当前日期
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        # 获取30天前的日期
        start_date = (datetime.datetime.now() - datetime.timedelta(days=40)).strftime('%Y-%m-%d')
        
        # 获取股票列表
        rs = bs.query_stock_basic()
        stock_list = []
        while (rs.error_code == '0') & rs.next():
            stock_list.append(rs.get_row_data())
        
        # 遍历每只股票
        for stock in stock_list:
            stock_code = stock[0]  # 股票代码
            
            # 获取股票数据
            df = get_stock_data(stock_code, start_date, end_date)
            
            # 检查是否满足条件
            if check_volume_conditions(df):
                save_results(conn, stock_code, end_date)
            
            # 添加延时，避免请求过于频繁
            time.sleep(0.5)
            
    finally:
        # 登出系统
        bs.logout()
        conn.close()

if __name__ == "__main__":
    main()
