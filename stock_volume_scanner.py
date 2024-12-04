import pymysql
from config import DB_CONFIG
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import concurrent.futures
from typing import List, Dict
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class StockVolumeScanner:
    def __init__(self, thread_workers=4):
        self.thread_workers = thread_workers
        self.conn = pymysql.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database']
        )
        self.cursor = self.conn.cursor()

    def get_stock_codes(self) -> List[str]:
        """获取所有股票代码"""
        query = """
        SELECT DISTINCT code 
        FROM stock_kline 
        WHERE date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
        """
        self.cursor.execute(query)
        return [row[0] for row in self.cursor.fetchall()]

    def get_stock_data(self, code: str, days=90) -> pd.DataFrame:
        """获取指定股票最近days天的数据"""
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        query = """
        SELECT code, date, open, high, low, close, volume, amount, turn
        FROM stock_kline
        WHERE code = %s AND date BETWEEN %s AND %s
        ORDER BY date
        """
        
        self.cursor.execute(query, (code, start_date, end_date))
        columns = ['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'turn']
        data = pd.DataFrame(list(self.cursor.fetchall()), columns=columns)
        return data

    def calculate_ma(self, group: pd.DataFrame, window: int) -> pd.Series:
        """计算移动平均线"""
        return group['close'].rolling(window=window).mean()

    def analyze_single_stock(self, code: str) -> Dict:
        """分析单个股票"""
        try:
            group = self.get_stock_data(code)
            if len(group) < 90:  # 确保有足够的数据
                return None
                
            # 计算MA10和MA20
            group['MA10'] = self.calculate_ma(group, 10)
            group['MA20'] = self.calculate_ma(group, 20)
            
            # 获取最近10天的数据
            recent_data = group.tail(10)
            old_data = group[:-10]
            
            if len(old_data) < 30:
                return None
            
            # 计算之前的平均成交量和标准差
            avg_volume_before = old_data['volume'].mean()
            volume_cv = old_data['volume'].std() / old_data['volume'].mean()
            
            # 1. 检查之前是否横盘（成交量稳定）
            if volume_cv > 0.5:
                return None
            
            # 2&3. 检查最近10天的成交量
            recent_volumes = recent_data['volume'].values
            max_recent_volume = recent_volumes.max()
            
            # 检查是否有3倍量
            if max_recent_volume < avg_volume_before * 3:
                return None
            
            # 检查成交量趋势
            volume_trend = np.polyfit(range(len(recent_volumes)), recent_volumes, 1)[0]
            if volume_trend <= 0:
                return None
            
            # 4. 检查价格是否在均线附近
            latest_price = group['close'].iloc[-1]
            latest_ma10 = group['MA10'].iloc[-1]
            latest_ma20 = group['MA20'].iloc[-1]
            
            deviation = 0.03
            near_ma = (abs(latest_price - latest_ma10) / latest_ma10 <= deviation or 
                      abs(latest_price - latest_ma20) / latest_ma20 <= deviation)
            
            if not near_ma:
                return None
            
            return {
                'code': code,
                'latest_date': group['date'].iloc[-1],
                'latest_price': latest_price,
                'volume_increase': max_recent_volume / avg_volume_before,
                'ma10': latest_ma10,
                'ma20': latest_ma20,
                'volume_trend': volume_trend
            }
        except Exception as e:
            logging.error(f"处理股票 {code} 时发生错误: {str(e)}")
            return None

    def scan_volume_patterns(self) -> pd.DataFrame:
        """使用多线程扫描符合条件的股票"""
        stock_codes = self.get_stock_codes()
        results = []
        
        logging.info(f"开始扫描 {len(stock_codes)} 只股票...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.thread_workers) as executor:
            future_to_code = {executor.submit(self.analyze_single_stock, code): code 
                            for code in stock_codes}
            
            for future in concurrent.futures.as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                        logging.info(f"股票 {code} 符合条件")
                except Exception as e:
                    logging.error(f"处理股票 {code} 时发生错误: {str(e)}")
        
        return pd.DataFrame(results)

    def __del__(self):
        self.cursor.close()
        self.conn.close()

if __name__ == '__main__':
    scanner = StockVolumeScanner(thread_workers=8)  # 使用8个线程
    results = scanner.scan_volume_patterns()
    
    if len(results) > 0:
        print(f"\n找到 {len(results)} 只符合条件的股票：")
        print(results.to_string(index=False))
    else:
        print("\n没有找到符合条件的股票。")
