import os
import time
import datetime
import sqlite3
import pandas as pd
import yfinance as yf
import requests
import duckdb  # 引入 DuckDB
from concurrent.futures import ThreadPoolExecutor

# 1. 数据库配置
# 现在的连接非常简单，数据会保存在项目根目录的 yahoo_stock_data.duckdb 文件里
DB_PATH = "yahoo_stock_data.duckdb"
con = duckdb.connect(DB_PATH)

# 2. Requests 逐个抓取月线
def download_via_requests(ticker, start_date, end_date):
    """当 yfinance 失败时，使用此函数作为保底"""
    try:
        table_name = ticker.lower().replace('.', '_').replace('-', '_')
        start_unix = int(time.mktime(time.strptime(start_date, "%Y-%m-%d")))
        end_unix = int(time.mktime(time.strptime(end_date, "%Y-%m-%d")))
        
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
                    # 直接将 df 存入数据库
                    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
                    con.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM df")
                    return True
    except Exception as e:
        print(f"   ❌ Requests 补录失败 [{ticker}]: {e}")
    return False

# 3. 核心下载逻辑
def download_chunk(ticker_list, start_date, end_date, method_choice):
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
                        
                        # DuckDB 批量入库
                        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
                        con.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM df")
                        
                        print(f"✅ yf 成功: {ticker}")
                        continue
                    
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

    elif method_choice == 2:
        for ticker in ticker_list:
            if download_via_requests(ticker, start_date, end_date):
                print(f"✅ API 成功: {ticker}")

# 4. 主程序控制
def download_main(market_option, method_option):
    market_map = {1: 'Shanghai_Shenzhen', 2: 'Snp500_Ru1000', 3: 'TSX'}
    start_date = "1970-01-01"
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')

    conn_local = sqlite3.connect('yahoo_data.db')
    targets = market_map.values() if market_option == 0 else [market_map.get(market_option)]
    
    print(f"🚀 启动月度下载 [ {method_option}]...")

    for m_name in targets:
        stocks = pd.read_sql(f"SELECT Yahoo_adj_Ticker_symbol FROM {m_name}", conn_local)['Yahoo_adj_Ticker_symbol'].tolist()
        
        chunk_size = 15 
        chunks = [stocks[i:i + chunk_size] for i in range(0, len(stocks), chunk_size)]
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            for chunk in chunks:
                executor.submit(download_chunk, chunk, start_date, end_date, method_option)
                time.sleep(1.2)
                
    conn_local.close()

if __name__ == '__main__':
    market_choice = 0  
    method_choice = 2 # 仅使用 Requests API 抓取
    
    download_main(market_choice, method_choice)
    print(f"\n🏁 同步结束: {datetime.datetime.now().strftime('%H:%M:%S')}")
