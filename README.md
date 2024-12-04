# Stock Pick

一个用于股票筛选和分析的Python工具集，主要功能包括K线分析、成交量筛选、日常数据更新等。

## 功能特点

- 自动化数据更新和维护
- K线形态分析和识别
- 成交量异常筛选
- 股票数据可视化
- 定时任务调度

## 主要模块

- `daily_update.py`: 每日数据更新模块
- `check_kline.py`: K线形态检查和分析
- `volume_screen.py`: 成交量筛选模块
- `stock_chart.py`: 股票图表绘制
- `stock_screener.py`: 股票筛选器
- `scheduler.py`: 定时任务调度

## 环境要求

- Python 3.7+
- 依赖包:
  - pandas >= 1.5.3
  - mplfinance >= 0.12.9b7
  - yfinance >= 0.2.18
  - pymysql >= 1.1.0
  - tqdm >= 4.65.0
  - numpy >= 1.24.0

## 安装说明

1. 克隆仓库
```bash
git clone https://github.com/[your-username]/stock_pick.git
cd stock_pick
```

2. 安装依赖
```bash
pip install -r requirements.txt
```

## 使用方法

1. 配置数据库（如果需要）
   - 执行 `create_volume_screen_table.sql` 创建必要的数据表

2. 运行日常更新
```bash
python daily_update.py
```

3. 运行成交量筛选
```bash
python volume_screen.py
```

4. 查看K线分析
```bash
python check_kline.py
```

## 日志文件

- `daily_update.log`: 记录每日更新的执行日志
- `volume_screen.log`: 记录成交量筛选的执行日志

## 注意事项

- 确保数据库配置正确
- 建议在市场收盘后运行数据更新
- 可以通过修改 `config.py` 来自定义配置

## License

[选择合适的开源协议]
