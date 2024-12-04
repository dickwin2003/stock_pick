import pymysql
import pandas as pd
from config import DB_CONFIG
from datetime import datetime, timedelta
import threading
from concurrent.futures import ThreadPoolExecutor
import logging
from queue import Queue
import time

# 设置日志配置
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('volume_screen.log'),
        logging.StreamHandler()
    ]
)

def connect_database():
    """连接到MySQL数据库"""
    logging.info(f"正在连接到数据库 {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    try:
        conn = pymysql.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database']
        )
        logging.info("数据库连接成功")
        return conn
    except Exception as e:
        logging.error(f"数据库连接失败: {str(e)}")
        raise

def get_stock_data(conn, stock_code, days=25):
    """从stock_kline表获取指定股票的历史数据"""
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    logging.info(f"获取股票 {stock_code} 的数据 - 时间范围: {start_date} 到 {end_date}")
    
    query = """
        SELECT date, code, amount 
        FROM stock_kline 
        WHERE code = %s 
        AND date BETWEEN %s AND %s 
        ORDER BY date ASC
    """
    
    df = pd.read_sql(query, conn, params=(stock_code, start_date, end_date))
    logging.info(f"股票 {stock_code} 获取到 {len(df)} 条数据记录")
    return df

def get_stock_name(conn, stock_code):
    """从stock_codes表获取股票名称"""
    cursor = conn.cursor()
    try:
        # 首先检查表结构
        cursor.execute("SHOW COLUMNS FROM stock_codes")
        columns = [col[0] for col in cursor.fetchall()]
        
        # 根据实际列名构建查询
        name_column = 'stock_name' if 'stock_name' in columns else 'code_name'
        sql = f"SELECT {name_column} FROM stock_codes WHERE code = %s"
        
        logging.debug(f"执行SQL查询股票名称: {sql} with code={stock_code}")
        cursor.execute(sql, (stock_code,))
        result = cursor.fetchone()
        if result:
            logging.debug(f"成功获取到股票名称: {result[0]}")
            return result[0]
        else:
            logging.warning(f"未找到股票 {stock_code} 的名称")
            return stock_code  # 如果找不到名称，就返回股票代码作为名称
    except Exception as e:
        logging.error(f"获取股票 {stock_code} 名称时出错: {str(e)}")
        return stock_code  # 发生错误时也返回股票代码作为名称
    finally:
        cursor.close()

def check_volume_conditions(df):
    """检查交易量是否满足条件"""
    stock_code = df['code'].iloc[0] if not df.empty else 'unknown'
    logging.debug(f"开始检查股票 {stock_code} 的交易量条件")
    
    if len(df) < 17:  # 确保有足够的数据
        logging.debug(f"股票 {stock_code} 数据量不足: 只有 {len(df)} 条记录，需要至少17条")
        return False
    
    # 获取最近2天的数据
    recent_2days = df.iloc[-2:]
    
    # 获取之前的数据（3-17天）
    previous_days = df.iloc[-17:-2]
    
    # 条件1：之前的交易量较少（使用平均值作为基准）
    avg_volume_previous = previous_days['amount'].mean()
    std_volume_previous = previous_days['amount'].std()
    relative_std = std_volume_previous / avg_volume_previous
    
    logging.debug(f"股票 {stock_code} 历史交易量分析:")
    logging.debug(f"- 平均值: {avg_volume_previous:,.2f}")
    logging.debug(f"- 标准差: {std_volume_previous:,.2f}")
    logging.debug(f"- 相对标准差: {relative_std:.2f}")
    
    # 检查之前的交易量是否不活跃（波动较小）
    if relative_std > 0.8:  # 相对标准差阈值
        logging.debug(f"股票 {stock_code} 相对标准差 {relative_std:.2f} > 0.8，波动性过大")
        return False
    
    # 条件2：最近2天的交易量都要大于之前平均值的3倍
    volume_threshold = avg_volume_previous * 3
    recent_volumes = recent_2days['amount'].values
    logging.debug(f"股票 {stock_code} 最近两天交易量分析:")
    logging.debug(f"- 最近两天交易量: {recent_volumes[0]:,.2f}, {recent_volumes[1]:,.2f}")
    logging.debug(f"- 需超过阈值(3倍均值): {volume_threshold:,.2f}")
    
    if not all(recent_2days['amount'] > volume_threshold):
        logging.debug(f"股票 {stock_code} 最近两天的交易量未能超过阈值")
        return False
    
    # 条件3：最近2天的交易量保持增长
    if not recent_volumes[0] < recent_volumes[1]:
        logging.debug(f"股票 {stock_code} 最近两天交易量未保持增长: {recent_volumes[0]:,.2f} -> {recent_volumes[1]:,.2f}")
        return False
    
    logging.info(f"股票 {stock_code} 满足所有交易量条件！")
    logging.info(f"- 过去平均交易量: {avg_volume_previous:,.2f}")
    logging.info(f"- 最近两天交易量: {recent_volumes[0]:,.2f} -> {recent_volumes[1]:,.2f}")
    return True

def save_results(conn, stock_code, scan_date):
    """保存筛选结果到数据库"""
    cursor = conn.cursor()
    try:
        # 获取股票名称
        logging.debug(f"开始获取股票 {stock_code} 的名称")
        stock_name = get_stock_name(conn, stock_code)
        logging.debug(f"获取到的股票名称: {stock_name}")
        
        # 插入数据
        sql = "INSERT INTO volume_screen_results (stock_code, stock_name, scan_date) VALUES (%s, %s, %s)"
        logging.debug(f"执行插入SQL: {sql} with values=({stock_code}, {stock_name}, {scan_date})")
        cursor.execute(sql, (stock_code, stock_name, scan_date))
        conn.commit()
        logging.info(f"成功保存股票 {stock_code} ({stock_name}) 的筛选结果")
    except Exception as e:
        logging.error(f"保存股票 {stock_code} 结果时出错: {str(e)}")
    finally:
        cursor.close()

def get_all_stock_codes(conn):
    """获取所有股票代码"""
    query = "SELECT DISTINCT code FROM stock_kline"
    df = pd.read_sql(query, conn)
    return df['code'].tolist()

def process_stock(stock_code, scan_date, progress_queue):
    """处理单个股票的函数"""
    try:
        conn = connect_database()
        df = get_stock_data(conn, stock_code)
        
        if check_volume_conditions(df):
            save_results(conn, stock_code, scan_date)
            logging.info(f"找到符合条件的股票: {stock_code}")
        
        progress_queue.put(1)  # 用于进度追踪
        conn.close()
        
    except Exception as e:
        logging.error(f"处理股票 {stock_code} 时出错: {str(e)}")
        progress_queue.put(1)

def main():
    logging.info("开始筛选股票...")
    start_time = time.time()
    
    try:
        # 连接数据库
        conn = connect_database()
        scan_date = datetime.now().strftime('%Y-%m-%d')
        
        # 获取所有股票代码
        stock_codes = get_all_stock_codes(conn)
        total_stocks = len(stock_codes)
        logging.info(f"共找到 {total_stocks} 只股票")
        
        # 确保表存在并清空之前的筛选结果
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS volume_screen_results (
                id INT AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(10),
                stock_name VARCHAR(50),
                scan_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_scan_date (scan_date),
                INDEX idx_stock_code (stock_code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        cursor.execute("TRUNCATE TABLE volume_screen_results")
        conn.commit()
        cursor.close()
        logging.info("已清空历史筛选结果")
        
        # 创建进度队列
        progress_queue = Queue()
        processed_count = 0
        
        # 使用线程池处理股票
        with ThreadPoolExecutor(max_workers=10) as executor:
            # 提交所有任务
            futures = [
                executor.submit(process_stock, stock_code, scan_date, progress_queue)
                for stock_code in stock_codes
            ]
            
            # 监控进度
            while processed_count < total_stocks:
                progress_queue.get()
                processed_count += 1
                if processed_count % 100 == 0:
                    elapsed_time = time.time() - start_time
                    progress = (processed_count / total_stocks) * 100
                    logging.info(f"进度: {processed_count}/{total_stocks} ({progress:.2f}%) - 已用时: {elapsed_time:.2f}秒")
        
        elapsed_time = time.time() - start_time
        logging.info(f"筛选完成！总用时: {elapsed_time:.2f}秒")
        
    except Exception as e:
        logging.error(f"程序执行出错: {str(e)}")

if __name__ == "__main__":
    main()
