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
DB_HOST = "pgm-7xvv5102g97m8i18ho.pg.rds.aliyuncs.com" # 您的外网地址
DB_PORT = "5432" # 外网端口
DB_NAME = "yahoo_stock_data" # 目标数据库

# 建立增强版连接引擎 (pool_pre_ping 确保连接断开时能自动重连)
conn_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_str, pool_pre_ping=True)

fail_download = {'Snp500_Ru1000': []}

def downloader(ticker, start_date, end_date):
    """
    下载逻辑：下载并直接存入云端数据库，自动处理表结构
    """
    try:
        # 下载数据 (修复某些版本 yfinance 默认下载多层索引的问题)
        data = yf.download(ticker, start=start_date, end=end_date)
        
        if data is not None and not data.empty:
            # --- 关键修正：将多层列标题简化为单层 ---
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            
            # 1. 整理表名：PostgreSQL 强制小写，且不能有特殊字符
            table_name = ticker.lower().replace('.', '_').replace('-', '_')
            
            # 2. 核心：存入数据库
            # if_exists='replace': 每次运行都重新覆盖，保证数据最全
            # method='multi': 开启批量写入模式，速度提升 5-10 倍
            data.to_sql(table_name, engine, if_exists='replace', index=True, method='multi')
            
            print(f"✅ 🚀 同步成功: {ticker} -> RDS 数据库表 [{table_name}]")
            return True
        else:
            print(f"⚠️ {ticker} 在雅虎财经中未找到数据")
    except Exception as e:
        print(f"❌ {ticker} 入库出错: {e}")
    return False

def download_main():
    print("--- 启动云端同步程序 ---")
    print(f"yfinance 版本: {yf.__version__}")
    
    # 从本地 SQLite 加载股票清单 (确保此文件在您的仓库里)
    print("正在加载股票清单...")
    try:
        conn = sqlite3.connect('yahoo_data.db')
        query = "SELECT Yahoo_adj_Ticker_symbol FROM master"
        data_df = pd.read_sql(query, conn)
        conn.close()
        
        stocks = data_df['Yahoo_adj_Ticker_symbol'].tolist()
        print(f"成功找到 {len(stocks)} 只待同步股票")
            
    except Exception as e:
        print(f"读取本地清单失败 (请检查 yahoo_data.db 是否存在): {e}")
        return

    # 设置下载日期范围
    start_date = "1970-01-01"
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')

    # 开始循环同步
    for i, ticker in enumerate(stocks):
        success = downloader(ticker, start_date, end_date)
        if not success:
            fail_download['Snp500_Ru1000'].append(ticker)
        
        # 每下载 10 个打印一次进度，防止日志太长
        if (i + 1) % 10 == 0:
            print(f"进度报告: 已处理 {i+1}/{len(stocks)}")
            
        # 频率控制：每只股票间隔 0.5 秒，既快又不被封 IP
        time.sleep(0.5)

    print("\n--- 所有任务执行完毕 ---")
    print(f"同步失败清单: {fail_download['Snp500_Ru1000']}")
    print(f"同步结束！失败数: {len(fail_download['Snp500_Ru1000'])}")

if __name__ == '__main__':
    start_time = time.time()
    download_main()
    total_time = time.time() - start_time
    print(f"🎉 任务完成！总耗时: {total_time:.2f}秒")

