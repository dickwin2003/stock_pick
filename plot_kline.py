import pandas as pd
import mplfinance as mpf
import pymysql
from datetime import datetime
from config import DB_CONFIG

# 连接数据库
conn = pymysql.connect(
    host=DB_CONFIG['host'],
    port=DB_CONFIG['port'],
    user=DB_CONFIG['user'],
    password=DB_CONFIG['password'],
    database=DB_CONFIG['database']
)

# 从数据库读取数据
query = "SELECT * FROM stock_kline ORDER BY trade_date"
df = pd.read_sql(query, conn)

# 关闭数据库连接
conn.close()

# 转换日期列为datetime类型
df['trade_date'] = pd.to_datetime(df['trade_date'])

# 重命名列以符合mplfinance要求
df = df.rename(columns={
    'trade_date': 'Date',
    'open': 'Open',
    'high': 'High',
    'low': 'Low',
    'close': 'Close',
    'volume': 'Volume'
})

# 设置日期为索引
df.set_index('Date', inplace=True)

# 绘制K线图
mpf.plot(df,
         type='candle',
         title='Stock K-Line Chart',
         volume=True,
         style='yahoo',
         figsize=(15, 10))
