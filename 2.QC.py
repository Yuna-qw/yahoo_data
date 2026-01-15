import os
import pandas as pd
import datetime
from sqlalchemy import create_engine, text

# --- 1. 数据库配置 ---
DB_USER = "yu"
DB_PASSWORD = os.getenv('DB_PASSWORD', 'Yahoo1223')
DB_HOST = "pgm-7xvv5102g97m8i18ho.pg.rds.aliyuncs.com"
DB_PORT = "5432"
DB_NAME = "yahoo_stock_data"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def run_fast_qc():
    print(f"🚀 启动QC... {datetime.datetime.now()}")
    
    # 用一条 SQL 统计所有表的行数（依靠 Postgres 统计信息）
    # 注意：reltuples 是估算行数，速度极快；MAX 日期仍需抽样查询
    query = """
    SELECT 
        relname as table_name, 
        n_live_tup as row_count_estimate
    FROM pg_stat_user_tables 
    WHERE schemaname = 'public';
    """
    
    with engine.connect() as conn:
        df_tables = pd.read_sql(query, conn)
    
    results = []
    today = datetime.datetime.now()
    
    print(f"检测到 {len(df_tables)} 张表，正在核对日期...")

    for idx, row in df_tables.iterrows():
        table = row['table_name']
        est_rows = row['row_count_estimate']
        
        # 只对有数据的表查最后日期，防止空跑
        last_date_str = "N/A"
        status = "✅ OK"
        
        try:
            if est_rows == 0:
                status = "❌ Empty"
            else:
                # 仅查询最后一行日期
                with engine.connect() as conn:
                    last_dt = conn.execute(text(f'SELECT MAX("Date") FROM "{table}"')).scalar()
                
                if last_dt:
                    last_date = pd.to_datetime(last_dt)
                    last_date_str = last_date.strftime('%Y-%m-%d')
                    # 月度逻辑判定：超过 35 天没更新算 Stale
                    if (today - last_date).days > 35:
                        status = "⚠️ Stale"
                else:
                    status = "❌ Empty"
        except Exception as e:
            status = "🚨 Error"

        results.append({
            "Ticker": table,
            "Status": status,
            "Last_Date": last_date_str,
            "Est_Rows": est_rows
        })
        
        # 每处理 100 张表打印一次，防止 GitHub 觉得我们卡死了
        if idx % 100 == 0:
            print(f"进度: {idx}/{len(df_tables)}...")

    # 保存报表
    df_res = pd.DataFrame(results)
    df_res.to_csv('QC_Monthly_Logic_Report.csv', index=False)
    df_res[df_res['Status'] != "✅ OK"].to_csv('QC_Monthly_Issues.csv', index=False)
    print("✅ QC 完成！报告已生成。")

if __name__ == '__main__':
    run_fast_qc()
