import os
import time
import datetime
import sqlite3
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine

print(f"yfinance 版本: {yf.__version__}")

# --- 数据库连接配置  ---
DB_USER = "yu" 
DB_PASSWORD = "Yahoo1223" 
DB_HOST = "pgm-7xvv5102g97m8i18ho.pg.rds.aliyuncs.com"
DB_PORT = "5432"
DB_NAME = "yahoo_stock_data"

# 建立 PostgreSQL 传送带
conn_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_str)

fail_download = {'Snp500_Ru1000': []}

def downloader(ticker, start_date, end_date):
    """
    下载逻辑：下载并直接存入云端数据库
    """
    try:
        # 下载数据
        data = yf.download(ticker, start=start_date, end=end_date)
        
        if data is not None and not data.empty:
            # 1. 整理表名：PostgreSQL 表名建议小写
            table_name = ticker.lower().replace('.', '_')
            
            # 2. 核心：存入数据库
            # if_exists='replace' 表示每次都更新成最新的全量数据
            data.to_sql(table_name, engine, if_exists='replace', index=True)
            
            print(f"✅ 同步成功: {ticker} -> 云端数据库")
            return True
        else:
            print(f"⚠️ {ticker} 无数据")
    except Exception as e:
        print(f"❌ {ticker} 下载或入库出错: {e}")
    return False

def download_main():
    print("正在从本地 SQLite 加载股票清单...")
    try:
        conn = sqlite3.connect('yahoo_data.db')
        query = "SELECT Yahoo_adj_Ticker_symbol FROM master"
        data_df = pd.read_sql(query, conn)
        conn.close()
        
        stocks = data_df['Yahoo_adj_Ticker_symbol'].tolist()
        print(f"成功找到 {len(stocks)} 只股票")
            
    except Exception as e:
        print(f"读取 SQLite 数据库失败: {e}")
        return

    start_date = "1970-01-01"
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')

    # 开始同步到云端
    for ticker in stocks:
        success = downloader(ticker, start_date, end_date)
        if not success:
            fail_download['Snp500_Ru1000'].append(ticker)
        # 频率稍微降低一点，保护您的数据库
        time.sleep(1) 

    print(f"同步结束！失败数: {len(fail_download['Snp500_Ru1000'])}")

if __name__ == '__main__':
    start_time = time.time()
    download_main()
    print(f"总耗时: {time.time() - start_time:.2f}秒")
