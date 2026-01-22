# coding: utf-8
import os
import openpyxl
import pandas as pd
import datetime
from sqlalchemy import create_engine, text

# --- 1. 数据库配置 ---
DB_USER = "yu"
DB_PASSWORD = os.getenv('DB_PASSWORD', 'Yahoo1223')
DB_HOST = "pgm-7xvv5102g97m8i18ho.pg.rds.aliyuncs.com"
DB_PORT = "5432"
DB_NAME = "yahoo_stock_data"

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_timeout=30
)

# --- 2. 日期逻辑保留 ---
now = datetime.datetime.now()
# 判定基准：上月最后一天
end_dt = datetime.datetime(now.year, now.month, 1) - datetime.timedelta(days=1)
endDate = end_dt.strftime('%Y-%m-%d')  # 格式如: 2025-12-31

if end_dt.month >= 10:
    upDate = f"{end_dt.year}.{end_dt.month}"
else:
    upDate = f"{end_dt.year}.0{end_dt.month}"

LOCAL_DB = "yahoo_data.db"

def get_data_from_sqlite():
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

def check_db_date(table_name):
    """去数据库查最新日期"""
    try:
        query = text(f'SELECT "date" FROM "{table_name}" ORDER BY "date" DESC LIMIT 1')
        with engine.connect() as conn:
            res = conn.execute(query).fetchone()
        if res and res[0]:
            # 统一转为 YYYY-MM-DD 字符串
            return res[0].strftime('%Y-%m-%d') if isinstance(res[0], (datetime.date, datetime.datetime)) else str(res[0])[:10]
        return None
    except:
        return "Error"

def sum():
    """汇总"""
    report_file = f"QC_report_{upDate}.xlsx"
    
    # 初始化 Excel 报告
    wb = openpyxl.Workbook()
    s = wb.active
    s.title = "Summary_cnt"
    s['A1'], s['B1'], s['C1'], s['D1'], s['E1'] = "country", "tickers of master_sheet", "threshold", "total downloaded", endDate
    
    n = 0
    for country in countries:
        print(f"正在检查市场: {country} ...")
        t_mus = 0    # 主表要求下载的总数
        t_down = 0   # 数据库中存在的表数量
        dow_yes = 0  # 日期正确的数量
        
        # 遍历主表中的每一行
        for index, row in _data[n].iterrows():
            if row['currently use'] == 'yes':
                t_mus += 1
                ticker = row['Yahoo_adj_Ticker_symbol'] # 假设列名是这个
                
                # 去数据库查日期
                db_date = check_db_date(ticker)
                
                if db_date and db_date != "Error":
                    t_down += 1
                    if db_date == endDate:
                        dow_yes += 1
        
        # 写入 Excel
        row_idx = n + 2
        s['A' + str(row_idx)] = country
        s['B' + str(row_idx)] = t_mus
        s['C' + str(row_idx)] = int(0.9 * t_mus)
        s['D' + str(row_idx)] = t_down
        s['E' + str(row_idx)] = dow_yes
        n += 1

    wb.save(report_file)
    print(f"✅ QC 汇总完成，报告已生成: {report_file}")

if __name__ == '__main__':
    sum()
