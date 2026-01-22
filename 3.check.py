# coding: utf-8
import os
import openpyxl
import pandas as pd
import datetime
import sqlite3
from sqlalchemy import create_engine, text

# --- 1. 远程 RDS 数据库配置 ---
RDS_USER = "yu"
RDS_PASSWORD = os.getenv('DB_PASSWORD', 'Yahoo1223')
RDS_HOST = "pgm-7xvv5102g97m8i18ho.pg.rds.aliyuncs.com"
RDS_PORT = "5432"
RDS_NAME = "yahoo_stock_data"

rds_engine = create_engine(
    f"postgresql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_NAME}",
    pool_timeout=30
)

# --- 2. 本地 SQLite 配置 ---
LOCAL_DB = "yahoo_data.db"

def get_data_from_sqlite():
    """从本地 sqlite 读表"""
    conn = sqlite3.connect(LOCAL_DB)
    try:
        data_1 = pd.read_sql("SELECT * FROM Shanghai_Shenzhen", conn)
        data_2 = pd.read_sql("SELECT * FROM Snp500_Ru1000", conn)
        data_3 = pd.read_sql("SELECT * FROM TSX", conn)
        return [data_1, data_2, data_3]
    except Exception as e:
        print(f"❌ 读取本地数据库失败: {e}")
        return [pd.DataFrame(), pd.DataFrame(), pd.DataFrame()]
    finally:
        conn.close()

# --- 3. 初始化清单 ---
_data = get_data_from_sqlite()
countries = ['Shanghai_Shenzhen', 'Snp500_Ru1000', 'TSX']

# --- 4. 日期逻辑 ---
now = datetime.datetime.now()
end_dt = datetime.datetime(now.year, now.month, 1) - datetime.timedelta(days=1)
endDate = end_dt.strftime('%Y-%m-%d')
upDate = end_dt.strftime('%Y.%m')

def check_rds_date(table_name):
    """去远程 RDS 查每张表的最新日期"""
    try:
        fixed_name = table_name.lower().replace('.', '_')
        query = text(f'SELECT "date" FROM "{fixed_name}" ORDER BY "date" DESC LIMIT 1')
        with rds_engine.connect() as conn:
            res = conn.execute(query).fetchone()
        if res and res[0]:
            return res[0].strftime('%Y-%m-%d') if isinstance(res[0], (datetime.date, datetime.datetime)) else str(res[0])[:10]
        return None
    except:
        return None # 如果 RDS 里没有这张表，返回 None

def sum():
    report_file = f"QC_report_{upDate}.xlsx"
    wb = openpyxl.Workbook()
    s = wb.active
    s.title = "Summary_cnt"
    s.append(["country", "tickers in local db", "threshold", "total in RDS", endDate])
    
    for n, country in enumerate(countries):
        df_list = _data[n]
        if df_list.empty:
            continue
            
        print(f"🔍 正在核对市场: {country} ...")
        t_mus = len(df_list) # 本地数据库里有多少只股票
        t_down = 0           # 远程 RDS 存在的表
        dow_yes = 0          # 日期正确的表
        
        for index, row in df_list.iterrows():
            ticker = row['Yahoo_adj_Ticker_symbol']
            
            db_date = check_rds_date(ticker)
            if db_date:
                t_down += 1
                if db_date == endDate:
                    dow_yes += 1
        
        s.append([country, t_mus, int(0.9 * t_mus), t_down, dow_yes])

    wb.save(report_file)
    print(f"✅ 完成！清单来自 {LOCAL_DB}，质检结果已生成: {report_file}")

if __name__ == '__main__':
    sum()

