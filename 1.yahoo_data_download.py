import os
import time
import datetime
import sqlite3
import pandas as pd
import yfinance as yf
import requests
import duckdb  
from concurrent.futures import ThreadPoolExecutor

# 1. 数据库配置
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
    for ticker in ticker_list:
        try:
            success = False
            #  yfinance
            if method_choice in [0, 1]:
                # 增加 timeout 参数，防止死等
                df = yf.download(ticker, start=start_date, end=end_date, 
                                 interval='1mo', progress=False, timeout=10)
                if not df.empty:
                    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
                    table_name = ticker.lower().replace('.', '_').replace('-', '_')
                    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
                    con.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM df")
                    print(f"✅ yf 成功: {ticker}")
                    success = True
            
            # 如果 yf 失败且模式允许，用 Requests 补录
            if not success and method_choice in [0, 2]:
                if download_via_requests(ticker, start_date, end_date):
                    print(f"✅ API 成功: {ticker}")
                else:
                    print(f"❌ {ticker} 完全失败")
            
            time.sleep(0.5)
            
        except Exception as e:
            print(f"⚠️ 处理 {ticker} 时出错: {e}")

# 4. 主程序控制
def download_main(market_option, method_option):
    market_map = {1: 'Shanghai_Shenzhen', 2: 'Snp500_Ru1000', 3: 'TSX'}
    start_date = "1970-01-01"
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')

    conn_local = sqlite3.connect('yahoo_data.db')
    targets = market_map.values() if market_option == 0 else [market_map.get(market_option)]
    
    print(f"🚀 启动月度下载 [模式 {method_option}]...")

    for m_name in targets:
        stocks = pd.read_sql(f"SELECT Yahoo_adj_Ticker_symbol FROM {m_name}", conn_local)['Yahoo_adj_Ticker_symbol'].tolist()
        
        print(f"📦 正在处理 {m_name}，共 {len(stocks)} 只股票...")
        download_chunk(stocks, start_date, end_date, method_option)
                
    conn_local.close()

if __name__ == '__main__':
    market_choice = 0  
    method_choice = 2 # 仅使用 Requests API 抓取
    
    download_main(market_choice, method_choice)
    print(f"\n🏁 同步结束: {datetime.datetime.now().strftime('%H:%M:%S')}")


