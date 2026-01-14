import os
import time
import datetime
import sqlite3
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine

# --- 1. 数据库连接配置 ---
DB_USER = "yu" 
DB_PASSWORD = os.getenv('DB_PASSWORD', 'Yahoo1223') # 优先读取 Secrets
DB_HOST = "pgm-7xvv5102g97m8i18ho.pg.rds.aliyuncs.com" 
DB_PORT = "5432" 
DB_NAME = "yahoo_stock_data"

conn_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_str, pool_pre_ping=True)

fail_download = {'Snp500_Ru1000': []}

def downloader(ticker, start_date, end_date):
    try:
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if data is not None and not data.empty:
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            
            table_name = ticker.lower().replace('.', '_').replace('-', '_')
            data.to_sql(table_name, engine, if_exists='replace', index=True, method='multi')
            return True
        else:
            print(f"  ⚠️  [跳过] {ticker}: 雅虎无数据")
    except Exception as e:
        print(f"  ❌  [错误] {ticker}: {e}")
    return False

def download_main():
    print("="*50)
    print(f"时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*50)
    
    try:
        conn = sqlite3.connect('yahoo_data.db')
        query = "SELECT Yahoo_adj_Ticker_symbol FROM Snp500_Ru1000"
        stocks = pd.read_sql(query, conn)['Yahoo_adj_Ticker_symbol'].tolist()
        conn.close()
        print(f"📂  成功加载清单: {len(stocks)} 只股票待处理")
    except Exception as e:
        print(f"🚨  清单读取失败: {e}")
        return

    start_date = "1970-01-01"
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    total = len(stocks)

    print("\n🛰️  开始同步至云端 RDS 数据库...")
    print("-" * 50)

    for i, ticker in enumerate(stocks):
        success = downloader(ticker, start_date, end_date)
        if not success:
            fail_download['Snp500_Ru1000'].append(ticker)
        
        if (i + 1) % 20 == 0 or (i + 1) == total:
            percent = ((i + 1) / total) * 100
            print(f"📊  进度: [{i+1}/{total}] {percent:>6.1f}% | 当前: {ticker:<6} | 写入正常")
            
        time.sleep(0.4) 

    print("-" * 50)
    if fail_download['Snp500_Ru1000']:
        print(f"📝  失败清单 ({len(fail_download['Snp500_Ru1000'])} 只): {fail_download['Snp500_Ru1000']}")
    print("="*50)

if __name__ == '__main__':
    start_time = time.time()
    download_main()
    print(f"✨  总耗时: {time.time() - start_time:.2f} 秒\n")
