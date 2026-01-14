import os
import time
import datetime
import sqlite3
import pandas as pd
import yfinance as yf
import requests
from sqlalchemy import create_engine

# --- 1. 数据库配置 (GitHub Secrets) ---
DB_USER = "yu"
DB_PASSWORD = os.getenv('DB_PASSWORD', 'Yahoo1223')
DB_HOST = "pgm-7xvv5102g97m8i18ho.pg.rds.aliyuncs.com"
DB_PORT = "5432"
DB_NAME = "yahoo_stock_data"

# 连接阿里云 RDS
conn_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_str, pool_pre_ping=True)

# 失败记录器
fail_download = {'Shanghai_Shenzhen': [], 'Snp500_Ru1000': [], 'TSX': []}

# --- 2. 核心下载与解析逻辑 ---
def downloader(ticker, market_name, start_date, end_date):
    # 统一云端表名为小写
    table_name = ticker.lower().replace('.', '_').replace('-', '_')
    
    # 策略 A: yfinance 优先
    try:
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if data is not None and not data.empty:
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            data.to_sql(table_name, engine, if_exists='replace', index=True, method='multi')
            return True
    except Exception:
        pass

    # 策略 B: Requests 备用解析 (当 yfinance 被限制或报错时触发)
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
                quote = chart.get("indicators", {}).get("quote", [{}])[0]
                adj = chart.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose", [])
                
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
                    return True
    except Exception as e:
        print(f"  ❌ {ticker} 下载彻底失败: {e}")
    
    return False

# --- 3. 单市场同步任务 ---
def run_sync(market_name):
    print(f"\n▶️ 开始同步市场: {market_name}")
    print("-" * 40)
    
    try:
        conn_local = sqlite3.connect('yahoo_data.db')
        # 直接匹配您本地数据库中的真实表名
        query = f"SELECT Yahoo_adj_Ticker_symbol FROM {market_name}"
        stocks = pd.read_sql(query, conn_local)['Yahoo_adj_Ticker_symbol'].tolist()
        conn_local.close()
    except Exception as e:
        print(f"🚨 读取清单失败: {e}")
        return

    start_date = datetime.datetime(1970, 1, 1)
    end_date = datetime.datetime.now()
    total = len(stocks)

    for i, ticker in enumerate(stocks):
        success = downloader(ticker, market_name, start_date, end_date)
        if not success:
            fail_download[market_name].append(ticker)
        
        if (i + 1) % 10 == 0 or (i + 1) == total:
            print(f"📈 [{market_name}] 进度: {i+1}/{total} | 当前: {ticker}")
        
        time.sleep(0.4) # 保护频率

# --- 4. 主入口控制 ---
def download_main(option):
    # 1=沪深, 2=标普, 3=加拿大
    market_map = {1: 'Shanghai_Shenzhen', 2: 'Snp500_Ru1000', 3: 'TSX'}
    
    start_time = time.time()
    
    if option == 0:
        print("🌟 模式：全市场全量同步启动！")
        for name in market_map.values():
            run_sync(name)
    elif option in market_map:
        run_sync(market_map[option])
    else:
        print("❌ 错误：无效的选项！请输入 0, 1, 2 或 3")

    print("\n" + "="*50)
    print(f"✅ 所有任务已完成！总耗时: {time.time() - start_time:.2f} 秒")
    for m, fails in fail_download.items():
        if fails:
            print(f"📝 {m} 失败清单: {fails}")
    print("="*50)

if __name__ == '__main__':
    # 0: 全部同步 | 1: 上海/深圳 | 2: 标普/罗素 | 3: 加拿大 (TSX)
    target_option = 0 
    download_main(target_option)
