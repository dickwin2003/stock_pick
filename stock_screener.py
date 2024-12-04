import pymysql
import pandas as pd
import numpy as np
from config import DB_CONFIG
from datetime import datetime, timedelta
import concurrent.futures
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class StockScreener:
    def __init__(self):
        self.conn = pymysql.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database']
        )
        
    def get_stock_data(self, stock_code):
        """获取指定股票最近90天的数据"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=120)  # 获取多一点数据以便计算均线
        
        query = """
            SELECT code, date, open, high, low, close, volume, amount, adjustflag, turn
            FROM stock_kline
            WHERE code = %s AND date >= %s
            ORDER BY date ASC
        """
        df = pd.read_sql(query, self.conn, params=(stock_code, start_date.strftime('%Y-%m-%d')))
        return df
    
    def calculate_moving_averages(self, df):
        """计算10日和20日均线"""
        df['MA10'] = df['close'].rolling(window=10).mean()
        df['MA20'] = df['close'].rolling(window=20).mean()
        return df
    
    def check_volume_stability(self, df, lookback_days=60):
        """检查过去几个月成交量是否稳定（横盘）"""
        if len(df) < lookback_days:
            return False
            
        recent_volume = df['volume'].iloc[-lookback_days:-10]  # 排除最近10天
        cv = recent_volume.std() / recent_volume.mean()
        return cv < 0.5  # 变异系数小于0.5认为相对稳定
    
    def check_volume_surge(self, df):
        """检查最近10天是否有3倍成交量且持续增长"""
        if len(df) < 30:
            return False
            
        # 计算基准成交量（最近10天之前的20天平均成交量）
        baseline_volume = df['volume'].iloc[-30:-10].mean()
        recent_volumes = df['volume'].tail(10)
        
        # 检查是否有3倍成交量
        has_surge = any(recent_volumes > baseline_volume * 3)
        
        # 检查最近10天成交量趋势
        volume_trend = np.polyfit(range(10), recent_volumes.values, 1)[0]
        
        return has_surge and volume_trend > 0
    
    def check_price_ma_position(self, df):
        """检查价格是否在10-20日均线附近"""
        if len(df) < 20:
            return False
            
        latest = df.iloc[-1]
        price = latest['close']
        ma10 = latest['MA10']
        ma20 = latest['MA20']
        
        # 价格在MA10和MA20之间，或者接近其中之一（上下3%范围内）
        near_ma10 = abs(price - ma10) / ma10 < 0.03
        near_ma20 = abs(price - ma20) / ma20 < 0.03
        between_mas = min(ma10, ma20) <= price <= max(ma10, ma20)
        
        return near_ma10 or near_ma20 or between_mas
    
    def screen_stock(self, stock_code):
        """筛选单个股票"""
        try:
            df = self.get_stock_data(stock_code)
            if len(df) < 60:  # 数据不足，跳过
                return None
                
            df = self.calculate_moving_averages(df)
            
            # 检查条件
            if (self.check_volume_stability(df) and 
                self.check_volume_surge(df) and 
                self.check_price_ma_position(df)):
                return {
                    'code': stock_code,
                    'latest_price': df['close'].iloc[-1],
                    'latest_volume': df['volume'].iloc[-1],
                    'avg_volume': df['volume'].iloc[-30:-10].mean(),
                    'date': df['date'].iloc[-1].strftime('%Y-%m-%d')
                }
        except Exception as e:
            logging.error(f"处理股票 {stock_code} 时出错: {str(e)}")
        return None

    def get_all_stock_codes(self):
        """获取所有股票代码"""
        query = "SELECT DISTINCT code FROM stock_kline"
        df = pd.read_sql(query, self.conn)
        return df['code'].tolist()

    def screen_stocks(self, max_workers=8):
        """多线程筛选股票"""
        stock_codes = self.get_all_stock_codes()
        results = []
        
        logging.info(f"开始筛选 {len(stock_codes)} 只股票...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.screen_stock, code): code for code in stock_codes}
            
            with tqdm(total=len(stock_codes), desc="筛选进度") as pbar:
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if result:
                        results.append(result)
                    pbar.update(1)
        
        return results

def main():
    screener = StockScreener()
    results = screener.screen_stocks()
    
    if results:
        print("\n符合条件的股票：")
        print(f"共找到 {len(results)} 只股票")
        for stock in results:
            print(f"\n代码: {stock['code']}")
            print(f"日期: {stock['date']}")
            print(f"最新价: {stock['latest_price']:.2f}")
            print(f"最新成交量: {stock['latest_volume']:,.0f}")
            print(f"基准成交量: {stock['avg_volume']:,.0f}")
    else:
        print("\n没有找到符合条件的股票")

if __name__ == "__main__":
    main()
