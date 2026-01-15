import os
import time
import datetime
import sqlite3
import pandas as pd
import yfinance as yf
import requests
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor

# --- 1. 数据库配置 ---
DB_USER = "yu"
DB_PASSWORD = os.getenv('DB_PASSWORD', 'Yahoo1223')
DB_HOST = "pgm-7xvv5102g97m8i18ho.pg.rds.aliyuncs.com"
DB_PORT = "5432"
DB_NAME = "yahoo_stock_data"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}", pool_pre_ping=True)

# --- 2. 备用引擎：Requests 逐个抓取月线 ---
def download_via_requests(ticker, start_date, end_date):
    """当 yfinance 失败时，使用此函数作为保底"""
    try:
        table_name = ticker.lower().replace('.', '_').replace('-', '_')
        # 将日期转换为 Unix 时间戳
        start_unix = int(time.mktime(time.strptime(start_date, "%Y-%m-%d")))
        end_unix = int(time.mktime(time.strptime(end_date, "%Y-%m-%d")))
        
        # 关键参数：range=max 或 period, interval=1mo
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?period1={start_unix}&period2={end_unix}&interval=1mo"
        headers = {"User-Agent": "Mozilla/5.0"}
        
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            result = response.json()
            chart = result.get("chart", {}).get("result", [None])[0]
            if chart and "timestamp" in chart:
                ts = chart.get("timestamp", [])
                indicators = chart.get("indicators", {})
                quote = indicators.get("quote", [{}])[0]
                adj = indicators.get("adjclose", [{}])[0].get("adjclose", [])
                
                df = pd.DataFrame({
                    "date": pd.to_datetime(ts, unit='s'),
                    "open": quote.get("open", []),
                    "high": quote.get("high", []),
                    "low": quote.get("low", []),
                    "close": quote.get("close", []),
                    "adj_close": adj,
                    "volume": quote.get("volume", [])
                }).dropna(subset=['close'])
                
                if not df.empty:
                    df.set_index("date", inplace=True)
                    df.to_sql(table_name, engine, if_exists='replace', index=True, method='multi')
                    return True
    except Exception as e:
        print(f"   ❌ Requests 补录失败 [{ticker}]: {e}")
    return False

# --- 3. 核心下载逻辑 ---
def download_chunk(ticker_list, start_date, end_date, method_choice):
    """
    method_choice: 1=仅yf, 2=仅Requests, 0=模式
    """
    # 策略 A: yfinance 批量
    if method_choice in [0, 1]:
        try:
            data = yf.download(ticker_list, start=start_date, end=end_date, interval='1mo', group_by='ticker', progress=False)
            for ticker in ticker_list:
                try:
                    df = data[ticker].dropna(subset=['Close']) if len(ticker_list) > 1 else data.dropna(subset=['Close'])
                    if not df.empty:
                        df.columns = [c.lower().replace(' ', '_') for c in df.columns]
                        table_name = ticker.lower().replace('.', '_').replace('-', '_')
                        df.to_sql(table_name, engine, if_exists='replace', index=True, method='multi')
                        print(f"✅ yf 成功: {ticker}")
                        continue # yf 成功了，跳过 Requests
                    
                    # 如果 yf 下回来是空的，且模式是混合，则尝试 Requests
                    if method_choice == 0:
                        if download_via_requests(ticker, start_date, end_date):
                            print(f"补✅ Requests 成功: {ticker}")
                except:
                    if method_choice == 0:
                        if download_via_requests(ticker, start_date, end_date):
                            print(f"补✅ Requests 成功: {ticker}")
        except Exception as e:
            print(f"⚠️ yf 批量组失败，尝试 Requests 逐个补录...")
            if method_choice == 0:
                for ticker in ticker_list:
                    if download_via_requests(ticker, start_date, end_date):
                        print(f"补✅ Requests 成功: {ticker}")

    # 策略 B: 仅 Requests
    elif method_choice == 2:
        for ticker in ticker_list:
            if download_via_requests(ticker, start_date, end_date):
                print(f"✅ API 成功: {ticker}")

# --- 4. 主程序控制 ---
def download_main(market_option, method_option):
    market_map = {1: 'Shanghai_Shenzhen', 2: 'Snp500_Ru1000', 3: 'TSX'}
    start_date = "1970-01-01"
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')

    conn_local = sqlite3.connect('yahoo_data.db')
    targets = market_map.values() if market_option == 0 else [market_map.get(market_option)]
    
    print(f"🚀 启动月度下载 [模式 {method_option}]...")

    for m_name in targets:
        stocks = pd.read_sql(f"SELECT Yahoo_adj_Ticker_symbol FROM {m_name}", conn_local)['Yahoo_adj_Ticker_symbol'].tolist()
        
        chunk_size = 15 # 稍微调小一点，增加稳定性
        chunks = [stocks[i:i + chunk_size] for i in range(0, len(stocks), chunk_size)]
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            for chunk in chunks:
                executor.submit(download_chunk, chunk, start_date, end_date, method_option)
                time.sleep(1.2)
                
    conn_local.close()

if __name__ == '__main__':
    # 0:全部跑, 1:沪深, 2:标普, 3:加拿大
    market_choice = 0  
    # 0: yf优先, 失败则Requests
    # 1: 仅 yfinance 
    # 2: 仅 Requests API
    method_choice = 2
    
    download_main(market_choice, method_choice)
    print(f"\n🏁 同步结束: {datetime.datetime.now().strftime('%H:%M:%S')}")
