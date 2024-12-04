import streamlit as st
import mysql.connector
from stock_chart import plot_candlestick
from config import DB_CONFIG

def get_stock_list():
    """
    从数据库获取所有可用的股票代码和名称
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        query = """
        SELECT DISTINCT k.code, c.code_name 
        FROM stock_kline k
        LEFT JOIN stock_codes c ON k.code = c.code
        ORDER BY k.code
        """
        cursor.execute(query)
        stocks = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return stocks
        
    except Exception as e:
        st.error(f"Error fetching stock list: {str(e)}")
        return []

def main():
    st.set_page_config(page_title="股票K线图查看器", layout="wide")
    
    # 页面标题
    st.title("股票K线图查看器")
    
    # 获取股票列表
    stock_list = get_stock_list()
    
    if not stock_list:
        st.error("无法获取股票列表，请检查数据库连接")
        return
    
    # 创建股票选择下拉框
    stock_dict = dict(stock_list)
    selected_stock = st.selectbox(
        "请选择股票",
        options=[stock[0] for stock in stock_list],
        format_func=lambda x: f"{x} - {stock_dict.get(x, '未知')}"
    )
    
    # 添加一个查看按钮
    if st.button("显示K线图"):
        # 获取选中股票的名称
        selected_stock_name = dict(stock_list).get(selected_stock, "未知")
        
        # 显示加载信息
        with st.spinner("正在加载数据..."):
            # 创建图表，添加股票名称到标题
            fig = plot_candlestick(selected_stock, 
                                 title=f"{selected_stock} - {selected_stock_name}",
                                 return_fig=True)
            if fig:
                # 显示图表
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.error("无法获取所选股票的数据")

if __name__ == "__main__":
    main()
