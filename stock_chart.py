import mysql.connector
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from config import DB_CONFIG
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import logging

# 设置日志配置
logging.basicConfig(level=logging.INFO)

def get_screened_stocks():
    """
    获取筛选后的股票列表
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 直接获取最新日期的筛选结果
        query = """
        SELECT v.stock_code, v.stock_name 
        FROM volume_screen_results v
        INNER JOIN (
            SELECT stock_code, MAX(scan_date) as latest_date
            FROM volume_screen_results
            GROUP BY stock_code
        ) latest ON v.stock_code = latest.stock_code 
        AND v.scan_date = latest.latest_date
        ORDER BY v.stock_code
        """
        cursor.execute(query)
        stocks = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        if not stocks:
            logging.warning("No screened stocks found in database")
        else:
            logging.info(f"Found {len(stocks)} screened stocks")
            
        return [(code, name) for code, name in stocks]
    
    except Exception as e:
        logging.error(f"Error getting screened stocks: {str(e)}")
        return []

def get_stock_data(stock_code):
    """
    从数据库获取股票数据，并包含股票名称
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 获取股票数据和名称
        query = """
        SELECT k.date, k.open, k.high, k.low, k.close, k.volume,
               v.stock_name
        FROM stock_kline k
        JOIN (
            SELECT stock_code, stock_name
            FROM volume_screen_results
            WHERE stock_code = %s
            AND scan_date = (
                SELECT MAX(scan_date)
                FROM volume_screen_results
                WHERE stock_code = %s
            )
        ) v ON k.code = v.stock_code
        WHERE k.code = %s
        ORDER BY k.date
        """
        cursor.execute(query, (stock_code, stock_code, stock_code))
        
        # 获取数据并转换为DataFrame
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'stock_name'])
        df['date'] = pd.to_datetime(df['date'])
        
        cursor.close()
        conn.close()
        
        return df
    
    except Exception as e:
        logging.error(f"Error getting data from database: {str(e)}")
        return None

def create_candlestick_figure(stock_code):
    """
    创建K线图的Figure对象
    """
    df = get_stock_data(stock_code)
    
    if df is None or df.empty:
        logging.warning("No data available for plotting")
        return go.Figure()
    
    # 使用股票名称作为标题
    stock_name = df['stock_name'].iloc[0] if 'stock_name' in df.columns else ''
    title = f'{stock_name} ({stock_code})'
    
    # 计算移动平均线
    df['MA10'] = df['close'].rolling(window=10).mean()
    df['MA20'] = df['close'].rolling(window=20).mean()
    
    # 创建子图布局
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                       vertical_spacing=0.03, 
                       row_heights=[0.7, 0.3])

    # 添加K线图到上方子图
    fig.add_trace(go.Candlestick(x=df['date'],
                                open=df['open'],
                                high=df['high'],
                                low=df['low'],
                                close=df['close'],
                                name='K线',
                                xaxis='x',
                                increasing_line_color='red',
                                decreasing_line_color='green'),
                  row=1, col=1)
    
    # 添加均线
    fig.add_trace(go.Scatter(x=df['date'],
                            y=df['MA10'],
                            line=dict(color='black', width=1),
                            name='MA10'),
                  row=1, col=1)
    
    fig.add_trace(go.Scatter(x=df['date'],
                            y=df['MA20'],
                            line=dict(color='blue', width=1),
                            name='MA20'),
                  row=1, col=1)
    
    # 添加成交量，设置红绿柱
    colors = ['red' if close > open else 'green' 
             for close, open in zip(df['close'], df['open'])]
    
    fig.add_trace(go.Bar(x=df['date'], 
                        y=df['volume'],
                        marker_color=colors,
                        name='成交量'),
                  row=2, col=1)
    
    # 设置图表布局
    fig.update_layout(
        title=dict(
            text=title,
            x=0.5,
            xanchor='center'
        ),
        height=800,
        xaxis_rangeslider_visible=False,
        yaxis=dict(
            title='价格',
            titlefont=dict(size=12),
            side='left',
            showgrid=True,
            gridwidth=1,
            gridcolor='LightGrey'
        ),
        yaxis2=dict(
            title='成交量',
            titlefont=dict(size=12),
            side='left',
            showgrid=True,
            gridwidth=1,
            gridcolor='LightGrey'
        ),
        margin=dict(l=50, r=50, t=50, b=50),
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor='rgba(255, 255, 255, 0.8)'
        ),
        template='plotly_white'
    )
    
    # 更新X轴设置，去除空白
    fig.update_xaxes(
        type='category',  # 使用分类模式
        categoryorder='category ascending',  # 确保日期顺序正确
        showgrid=True,
        gridwidth=1,
        gridcolor='LightGrey',
        showline=True,
        linewidth=1,
        linecolor='Grey',
        tickangle=45,  # 倾斜日期标签
        tickfont=dict(size=10),
        row=1, col=1
    )
    
    fig.update_xaxes(
        type='category',
        categoryorder='category ascending',
        showgrid=True,
        gridwidth=1,
        gridcolor='LightGrey',
        showline=True,
        linewidth=1,
        linecolor='Grey',
        tickangle=45,
        tickfont=dict(size=10),
        row=2, col=1
    )
    
    return fig

# 创建Dash应用
app = dash.Dash(__name__)

# 获取股票列表
stock_list = get_screened_stocks()
stock_options = [{'label': f'{code} - {name}', 'value': code} 
                 for code, name in stock_list]

# 设置应用布局
app.layout = html.Div([
    html.H1('股票K线图查看器', style={'textAlign': 'center'}),
    
    html.Div([
        html.Label('选择股票：'),
        dcc.Dropdown(
            id='stock-selector',
            options=stock_options,
            value=stock_options[0]['value'] if stock_options else None,
            style={'width': '100%'}
        ),
    ], style={'width': '50%', 'margin': '20px auto'}),
    
    dcc.Graph(id='candlestick-graph')
])

# 回调函数，更新K线图
@app.callback(
    Output('candlestick-graph', 'figure'),
    Input('stock-selector', 'value')
)
def update_graph(selected_stock):
    if not selected_stock:
        return go.Figure()
    return create_candlestick_figure(selected_stock)

if __name__ == '__main__':
    import socket
    
    # 获取本机IP地址
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    
    print(f"Starting server on {local_ip}:8050")
    print(f"You can access the application at: http://{local_ip}:8050")
    
    # 设置 host='0.0.0.0' 允许局域网访问
    app.run_server(debug=True, host='0.0.0.0', port=8050)
