import baostock as bs
import pandas as pd
import pymysql
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

def create_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Drop table if exists and create new one
    drop_table_sql = "DROP TABLE IF EXISTS stock_codes"
    cursor.execute(drop_table_sql)
    
    # Create table if not exists
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stock_codes (
        code VARCHAR(20) PRIMARY KEY,
        code_name VARCHAR(100),
        industry VARCHAR(100),
        trade_status VARCHAR(20),
        update_time DATETIME
    )
    """
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()

#### 登陆系统 ####
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

#### 获取证券信息 ####
rs = bs.query_all_stock(day="2017-06-30")
print('query_all_stock respond error_code:'+rs.error_code)
print('query_all_stock respond  error_msg:'+rs.error_msg)

#### 获取数据并存入MySQL ####
data_list = []
while (rs.error_code == '0') & rs.next():
    data_list.append(rs.get_row_data())
result = pd.DataFrame(data_list, columns=rs.fields)

# Create table
create_table()

# Insert data into MySQL
conn = get_db_connection()
cursor = conn.cursor()

# Insert new data
current_time = datetime.now()
for _, row in result.iterrows():
    sql = "INSERT INTO stock_codes (code, code_name, industry, trade_status, update_time) VALUES (%s, %s, %s, %s, %s)"
    cursor.execute(sql, (row['code'], row['code_name'], '', row['tradeStatus'], current_time))

conn.commit()
cursor.close()
conn.close()

print(f"Successfully stored {len(result)} stock codes in MySQL database")

#### 登出系统 ####
bs.logout()