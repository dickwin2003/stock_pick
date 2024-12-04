import pymysql
from datetime import datetime
from config import DB_CONFIG
import logging

logging.basicConfig(level=logging.INFO)

def check_today_kline():
    """检查今天的K线数据"""
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 检查今天的数据量
        sql = """
        SELECT DATE(date) as trade_date, COUNT(*) as count 
        FROM stock_kline 
        WHERE DATE(date) = %s 
        GROUP BY DATE(date)
        """
        cursor.execute(sql, (today,))
        result = cursor.fetchone()
        
        if result:
            logging.info(f"今天（{today}）的K线数据数量: {result[1]}条")
        else:
            logging.warning(f"今天（{today}）还没有K线数据")
            
        # 检查最新的数据日期
        sql = """
        SELECT DATE(date) as latest_date, COUNT(*) as count 
        FROM stock_kline 
        GROUP BY DATE(date) 
        ORDER BY DATE(date) DESC 
        LIMIT 1
        """
        cursor.execute(sql)
        result = cursor.fetchone()
        
        if result:
            logging.info(f"最新的K线数据日期是: {result[0]}，数据量: {result[1]}条")
        else:
            logging.warning("数据库中没有K线数据")
            
    except Exception as e:
        logging.error(f"检查数据时出错: {str(e)}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    check_today_kline()
