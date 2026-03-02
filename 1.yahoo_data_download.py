import os
import time
import datetime
import sqlite3
import pandas as pd
import yfinance as yf
import requests
import duckdb  

# 1. 数据库配置
DB_PATH = "yahoo_stock_data.duckdb"

con = duckdb.connect(DB_PATH)

# 2. 数据库入库核心逻辑
def save_to_duckdb(df, ticker):
    """统一处理 DuckDB 入库，解决数字表名和主键冲突问题"""
    if df is None or df.empty:
        return False
    
    try:
        table_name = ticker.lower().replace('.', '_').replace('-', '_')
        con.register('df_view', df)
        table_check = con.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0]
        
        if not table_check:
            con.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM df_view')
        else:
            con.execute(f'DELETE FROM "{table_name}"')
            con.execute(f'INSERT INTO "{table_name}" SELECT * FROM df_view')
            
        con.unregister('df_view')
        return True
    except Exception as e:
        print(f"   ❌ DuckDB 入库失败 [{ticker}]: {e}")
        return False

# 3. Requests 抓取逻辑
def download_via_requests(ticker, start_date, end_date):
    """API 模式下载"""
    try:
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
                
                # 统一列名格式
                df.columns = [c.lower() for c in df.columns]
                return save_to_duckdb(df, ticker)
    except Exception as e:
        print(f"   ❌ Requests 失败 [{ticker}]: {e}")
    return False

# 4. 核心下载控制
def download_chunk(ticker_list, start_date, end_date, method_choice):
    """单线程循环处理，防封防锁"""
    for ticker in ticker_list:
        try:
            success = False
            # yfinance
            if method_choice in [0, 1]:
                df = yf.download(ticker, start=start_date, end=end_date, 
                                 interval='1mo', progress=False, timeout=10)
                if not df.empty:
                    # 清洗 yfinance 特有的列名
                    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
                    # 重置索引，把 Date 变成普通列
                    df = df.reset_index()
                    df.columns = [c.lower() for c in df.columns]
                    
                    if save_to_duckdb(df, ticker):
                        print(f"✅ yf 成功: {ticker}")
                        success = True
            
            # Requests
            if not success and method_choice in [0, 2]:
                if download_via_requests(ticker, start_date, end_date):
                    print(f"✅ API 成功: {ticker}")
                else:
                    print(f"❌ {ticker} 失败")
            
            time.sleep(0.6)
            
        except Exception as e:
            print(f"⚠️ 处理 {ticker} 时出错: {e}")

# 5. 主程序
def download_main(market_option, method_option):
    market_map = {1: 'Shanghai_Shenzhen', 2: 'Snp500_Ru1000', 3: 'TSX'}
    start_date = "1970-01-01"
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')

    if not os.path.exists('yahoo_data.db'):
        print("❌ 找不到数据库 yahoo_data.db")
        return

    conn_local = sqlite3.connect('yahoo_data.db')
    targets = market_map.values() if market_option == 0 else [market_map.get(market_option)]
    
    print(f"🚀 启动下载 [模式 {method_option}]...")

    for m_name in targets:
        try:
            stocks = pd.read_sql(f"SELECT Yahoo_adj_Ticker_symbol FROM {m_name}", conn_local)['Yahoo_adj_Ticker_symbol'].tolist()
            print(f"正在处理 {m_name}，共 {len(stocks)} 只股票...")
            download_chunk(stocks, start_date, end_date, method_option)
        except Exception as e:
            print(f"⚠️ 读取 {m_name} 失败: {e}")
                
    conn_local.close()

if __name__ == '__main__':
    market_choice = 0  
    method_choice = 2 
    
    download_main(market_choice, method_choice)
    print(f"\n同步结束: {datetime.datetime.now().strftime('%H:%M:%S')}")
