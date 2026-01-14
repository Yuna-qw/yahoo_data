import os
import time
import datetime
import sqlite3
import pandas as pd
import yfinance as yf
import requests
from sqlalchemy import create_engine

# --- 1. 数据库配置 ---
DB_USER = "yu"
DB_PASSWORD = os.getenv('DB_PASSWORD', 'Yahoo1223') 
DB_HOST = "pgm-7xvv5102g97m8i18ho.pg.rds.aliyuncs.com" 
DB_PORT = "5432"
DB_NAME = "yahoo_stock_data"

# 创建数据库引擎
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}", pool_pre_ping=True)

# --- 2. 核心下载与入库逻辑 ---
def downloader(ticker, start_date, end_date):
    table_name = ticker.lower().replace('.', '_').replace('-', '_')
    
    try:
        data = yf.download(ticker, start=start_date, end=end_date, progress=False, threading=False)
        if data is not None and not data.empty:
            # 处理 yfinance 的多级索引列名
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            
            # 存入 RDS
            data.to_sql(table_name, engine, if_exists='replace', index=True, method='multi')
            # 只要这里成功运行，就打印并返回 True
            print(f"✅ Success (yfinance): {ticker}")
            return True
    except Exception:
        pass

    try:
        start_unix = int(time.mktime(start_date.timetuple()))
        end_unix = int(time.mktime(end_date.timetuple()))
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?period1={start_unix}&period2={end_unix}&interval=1d"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            result = response.json()
            chart = result.get("chart", {}).get("result", [None])[0]
            if chart:
                ts = chart.get("timestamp", [])
                indicators = chart.get("indicators", {})
                quote = indicators.get("quote", [{}])[0]
                adj = indicators.get("adjclose", [{}])[0].get("adjclose", [])
                
                # 构建数据表
                df = pd.DataFrame({
                    "Date": pd.to_datetime(ts, unit='s'),
                    "Open": quote.get("open", []),
                    "High": quote.get("high", []),
                    "Low": quote.get("low", []),
                    "Close": quote.get("close", []),
                    "Adj Close": adj,
                    "Volume": quote.get("volume", [])
                })
                
                # 清洗空数据并入库
                df = df.dropna(subset=['Close'])
                if not df.empty:
                    df.set_index("Date", inplace=True)
                    df.to_sql(table_name, engine, if_exists='replace', index=True, method='multi')
                    # 策略 B 成功，打印并返回
                    print(f"✅ Success (Requests): {ticker}")
                    return True
    except Exception:
        pass

    # 如果以上所有尝试都失败了，才打印 Failed
    print(f"❌ Failed: {ticker}")
    return False

# --- 3. 主程序控制 ---
def download_main(option):
    # 匹配本地数据库中的三张表
    market_map = {1: 'Shanghai_Shenzhen', 2: 'Snp500_Ru1000', 3: 'TSX'}
    
    # 设置下载范围（从1970年至今）
    start_date = datetime.datetime(1970, 1, 1)
    end_date = datetime.datetime.now()

    try:
        # 连接本地 SQLite 获取股票清单
        conn_local = sqlite3.connect('yahoo_data.db') 
        
        # 确定下载目标市场
        targets = market_map.values() if option == 0 else [market_map.get(option)]
        
        for m_name in targets:
            print(f"\n📂 正在处理市场表: {m_name}")
            # 读取对应市场的股票代码
            stocks = pd.read_sql(f"SELECT Yahoo_adj_Ticker_symbol FROM {m_name}", conn_local)['Yahoo_adj_Ticker_symbol'].tolist()
            
            for ticker in stocks:
                downloader(ticker, start_date, end_date)
                time.sleep(0.5) 
                
        conn_local.close()
    except Exception as e:
        print(f"🚨 运行出错: {e}")

if __name__ == '__main__':
    # 0:全部跑一遍, 1:沪深, 2:标普, 3:加拿大
    download_main(0)
    print(f"\n🏁 任务全部结束时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
