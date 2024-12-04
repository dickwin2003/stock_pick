import pymysql
from datetime import datetime, timedelta
from config import DB_CONFIG

def check_latest_data():
    # Connect to database
    conn = pymysql.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database']
    )
    
    try:
        cursor = conn.cursor()
        
        # Get yesterday's date
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Query to check data for yesterday
        query = f"""
        SELECT COUNT(*) as count, 
               MIN(code) as sample_code,
               MAX(update_time) as last_update
        FROM stock_kline 
        WHERE date = '{yesterday}'
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result:
            count, sample_code, last_update = result
            print(f"数据统计 (日期: {yesterday}):")
            print(f"- 总记录数: {count}")
            print(f"- 样本股票代码: {sample_code}")
            print(f"- 最后更新时间: {last_update}")
            
            if count > 0:
                # Get some sample data
                sample_query = f"""
                SELECT code, date, open, close, volume
                FROM stock_kline
                WHERE date = '{yesterday}'
                LIMIT 5
                """
                cursor.execute(sample_query)
                samples = cursor.fetchall()
                
                print("\n前5条记录示例:")
                for sample in samples:
                    print(f"股票: {sample[0]}, 开盘: {sample[2]}, 收盘: {sample[3]}, 成交量: {sample[4]}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    check_latest_data()
