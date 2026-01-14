import os
import time
import datetime
import sqlite3
import pandas as pd
import yfinance as yf
import requests
from sqlalchemy import create_engine

# --- 1. 数据库配置  ---
DB_USER = "yu"
DB_PASSWORD = os.getenv('DB_PASSWORD', 'Yahoo1223')
DB_HOST = "pgm-7xvv5102g97m8i18ho.pg.rds.aliyuncs.com"
DB_PORT = "5432"
DB_NAME = "yahoo_stock_data"

# 阿里云 RDS 连接引擎
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}", pool_pre_ping=True)

# --- 2. 核心下载引擎 ---
def downloader(ticker, start_date, end_date, use_requests=False):
    # 统一表名为小写规范
    table_name = ticker.lower().replace('.', '_').replace('-', '_')
    
    # 模式 A: 纯 yfinance 下载
    if not use_requests:
        try:
            data = yf.download(ticker, start=start_date, end=end_date, progress=False, threading=False)
            if data is not None and not data.empty:
                # 处理多级索引，确保列名纯净
                if isinstance(data.columns, pd.MultiIndex):
                    data.columns = data.columns.get_level_values(0)
                # 统一列名为小写且无空格
                data.columns = [c.lower().replace(' ', '_') for c in data.columns]
                data.to_sql(table_name, engine, if_exists='replace', index=True, method='multi')
                return True
        except:
            pass
    
    # 模式 B: 纯 Requests 下载
    else:
        try:
            start_unix = int(time.mktime(start_date.timetuple()))
            end_unix = int(time.mktime(end_date.timetuple()))
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?period1={start_unix}&period2={end_unix}&interval=1d"
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            if response.status_code == 200:
                result = response.json()
                chart = result.get("chart", {}).get("result", [None])[0]
                if chart:
                    ts = chart.get("timestamp", [])
                    indicators = chart.get("indicators", {})
                    quote = indicators.get("quote", [{}])[0]
                    adj = indicators.get("adjclose", [{}])[0].get("adjclose", [])
                    df = pd.DataFrame({
                        "date": pd.to_datetime(ts, unit='s'),
                        "open": quote.get("open", []), "high": quote.get("high", []),
                        "low": quote.get("low", []), "close": quote.get("close", []),
                        "adj_close": adj, "volume": quote.get("volume", [])
                    }).dropna(subset=['close'])
                    if not df.empty:
                        # 移除时区信息，防止入库报错
                        df['date'] = df['date'].dt.tz_localize(None)
                        df.set_index("date", inplace=True)
                        df.to_sql(table_name, engine, if_exists='replace', index=True, method='multi')
                        return True
        except:
            pass

    # 仅输出失败
    print(f"❌ Failed: {ticker}")
    return False

# --- 3. 主程序控制 ---
def download_main(market_option, use_requests_method=False):
    # 对应本地真实表名
    market_map = {1: 'Shanghai_Shenzhen', 2: 'Snp500_Ru1000', 3: 'TSX'}
    start_date = datetime.datetime(1970, 1, 1)
    end_date = datetime.datetime.now()

    if not os.path.exists('yahoo_data.db'):
        print("🚨 错误：找不到文件 yahoo_data.db")
        return

    conn_local = sqlite3.connect('yahoo_data.db')
    targets = market_map.values() if market_option == 0 else [market_map.get(market_option)]
    
    for m_name in targets:
        try:
            stocks = pd.read_sql(f"SELECT Yahoo_adj_Ticker_symbol FROM {m_name}", conn_local)['Yahoo_adj_Ticker_symbol'].tolist()
            for ticker in stocks:
                downloader(ticker, start_date, end_date, use_requests=use_requests_method)
                time.sleep(0.3) 
        except Exception as e:
            print(f"🚨 读取表 {m_name} 出错: {e}")
            
    conn_local.close()

if __name__ == '__main__':
    # 0:全部, 1:沪深, 2:标普, 3:加拿大
    market_choice = 0
    # False: yfinance (快) | True: Requests (稳)
    use_api = True 

    download_main(market_choice, use_api)
    print(f"🏁 同步结束: {datetime.datetime.now().strftime('%H:%M:%S')}")

