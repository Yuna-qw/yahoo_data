# coding: utf-8
import os
import openpyxl
import pandas as pd
import datetime
import sqlite3
import duckdb  # 替换 sqlalchemy

# 1. DuckDB 配置
DUCK_DB_PATH = "yahoo_stock_data.duckdb"
# 建立 DuckDB 连接
duck_con = duckdb.connect(DUCK_DB_PATH)

# 2. 本地 SQLite 配置
LOCAL_DB = "yahoo_data.db"

def get_data_from_sqlite():
    """从本地 sqlite 读表 (股票清单)"""
    conn = sqlite3.connect(LOCAL_DB)
    try:
        data_1 = pd.read_sql("SELECT * FROM Shanghai_Shenzhen", conn)
        data_2 = pd.read_sql("SELECT * FROM Snp500_Ru1000", conn)
        data_3 = pd.read_sql("SELECT * FROM TSX", conn)
        return [data_1, data_2, data_3]
    except Exception as e:
        print(f"❌ 读取本地清单数据库失败: {e}")
        return [pd.DataFrame(), pd.DataFrame(), pd.DataFrame()]
    finally:
        conn.close()

# 3. 初始化清单
_data = get_data_from_sqlite()
countries = ['Shanghai_Shenzhen', 'Snp500_Ru1000', 'TSX']

# 4. 日期逻辑
now = datetime.datetime.now()
# 逻辑：上个月最后一天
end_dt = datetime.datetime(now.year, now.month, 1) - datetime.timedelta(days=1)
endDate = end_dt.strftime('%Y-%m-%d')
upDate = end_dt.strftime('%Y.%m')

def check_duckdb_date(table_name):
    """去本地 DuckDB 查每张表的最新日期"""
    try:
        # 统一表名格式
        fixed_name = table_name.lower().replace('.', '_').replace('-', '_')
        # 执行查询
        query = f'SELECT "date" FROM "{fixed_name}" ORDER BY "date" DESC LIMIT 1'
        res = duck_con.execute(query).fetchone()
        
        if res and res[0]:
            # DuckDB 返回的日期对象直接转换
            raw_date = res[0]
            if isinstance(raw_date, (datetime.date, datetime.datetime)):
                return raw_date.strftime('%Y-%m-%d')
            return str(raw_date)[:10]
        return None
    except:
        return None # 如果库里没有这张表，报错跳过

def generate_report():
    report_file = f"QC_Full_Report_{upDate}.xlsx"
    wb = openpyxl.Workbook()
    s = wb.active
    s.title = "Summary_cnt"
    # 表头：国家，本地清单数，达标线(90%)，DuckDB实际存有的表，日期达标的表
    s.append(["country", "tickers in local list", "threshold(90%)", "found in DuckDB", "date matches " + endDate])
    
    for n, country in enumerate(countries):
        df_list = _data[n]
        if df_list.empty:
            continue
            
        print(f"🔍 正在核对: {country} (清单共 {len(df_list)} 只) ...")
        t_mus = len(df_list) # 清单里有多少只股票
        t_found = 0          # DuckDB 仓库里真实存在的表
        dow_yes = 0          # 日期更新正确的表
        
        for index, row in df_list.iterrows():
            ticker = row['Yahoo_adj_Ticker_symbol']
            
            db_date = check_duckdb_date(ticker)
            if db_date:
                t_found += 1
                if db_date >= endDate:
                    dow_yes += 1
        
        # 写入一行统计数据
        s.append([country, t_mus, int(0.9 * t_mus), t_found, dow_yes])

    wb.save(report_file)
    print(f"\n" + "="*50)
    print(f"✅ 质检完成！")
    print(f"📂 清单来源: {LOCAL_DB}")
    print(f"🗄️ 仓库来源: {DUCK_DB_PATH}")
    print(f"📊 报告已生成: {report_file}")
    print(f"="*50)

if __name__ == '__main__':
    generate_report()
