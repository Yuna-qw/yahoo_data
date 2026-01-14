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

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}", 
    pool_pre_ping=True,
    pool_recycle=3600
)

def downloader(ticker, start_date, end_date):
    table_name = ticker.lower().replace('.', '_').replace('-', '_')
    
    # 策略 A: yfinance
    try:
        data = yf.download(ticker, start=start_date, end=end_date, progress=False, threading=False)
        if data is not None and not data.empty:
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            data.to_sql(table_name, engine, if_exists='replace', index=True, method='multi', chunksize=500)
            print(f"✅ Success (yf): {ticker}") # 实时打印进度
            return True
    except Exception:
        pass

    # 策略 B: Requests 备用
    try:
        start_unix = int(time.mktime(start_date.timetuple()))
        end_unix = int(time.mktime(end_date.timetuple()))
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?period1={start_unix}&period2={end_unix}&interval=1d"
        headers = {"User-Agent": "Mozilla/5.0"}
        
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            result = response.json()
            chart = result.get("chart", {}).get("result", [None])[0]
            if chart:
                ts = chart.get("timestamp", [])
                indicators = chart.get("indicators", {})
                quote = indicators.get("quote", [{}])[0]
                adj = indicators.get("adjclose", [{}])[0].get("adjclose", [])
                
                df = pd.DataFrame({
                    "Date": pd.to_datetime(ts, unit='s'),
                    "Open": quote.get("open", []),
                    "High": quote.get("high", []),
                    "Low": quote.get("low", []),
                    "Close": quote.get("close", []),
                    "Adj Close": adj,
                    "Volume": quote.get("volume", [])
                }).dropna(subset=['Close'])
                
                if not df.empty:
                    df.set_index("Date", inplace=True)
                    df.to_sql(table_name, engine, if_exists='replace', index=True, method='multi')
                    print(f"✅ Success (req): {ticker}") # 实时打印进度
                    return True
    except Exception:
        pass

    print(f"❌ Failed: {ticker}")
    return False

def download_main(option):
    market_map = {1: 'Shanghai_Shenzhen', 2: 'Snp500_Ru1000', 3: 'TSX'}
    start_date = datetime.datetime(1970, 1, 1)
    end_date = datetime.datetime.now()

    try:
        conn_local = sqlite3.connect('yahoo_data.db')
        targets = market_map.values() if option == 0 else [market_map.get(option)]
        
        for m_name in targets:
            print(f"\n🚀 开始同步市场: {m_name}")
            stocks = pd.read_sql(f"SELECT Yahoo_adj_Ticker_symbol FROM {m_name}", conn_local)['Yahoo_adj_Ticker_symbol'].tolist()
            for ticker in stocks:
                downloader(ticker, start_date, end_date)
                time.sleep(0.5) 
                
        conn_local.close()
    except Exception as e:
        print(f"🚨 运行出错: {e}")

if __name__ == '__main__':
    download_main(0)
    print(f"🏁 任务结束: {datetime.datetime.now().strftime('%H:%M:%S')}")
