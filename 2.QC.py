import os
import datetime
import pandas as pd
from sqlalchemy import create_engine, text

# --- 1. 数据库配置 ---
DB_USER = "yu"
DB_PASSWORD = os.getenv('DB_PASSWORD', 'Yahoo1223')
DB_HOST = "pgm-7xvv5102g97m8i18ho.pg.rds.aliyuncs.com"
DB_PORT = "5432"
DB_NAME = "yahoo_stock_data"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def run_monthly_logic_qc():
    print(f'🚀 开始执行QC判定... 当前时间: {datetime.datetime.now().strftime("%Y-%m-%d")}')
    
    # 获取所有表名
    with engine.connect() as conn:
        tables = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")).fetchall()
    
    all_tables = [t[0] for t in tables]
    results = []
    today = datetime.datetime.now()

    for table in all_tables:
        status = "✅ OK"
        detail = ""
        last_date_str = "N/A"
        monthly_count = 0
        
        try:
            # 利用 Postgres 的 date_trunc 函数，找出每个月最后的一条记录
            # 逻辑：按月份分组，取每组中 Date 最大（最后一天）的那行
            monthly_query = text(f"""
                SELECT COUNT(*) FROM (
                    SELECT MAX("Date") 
                    FROM {table} 
                    GROUP BY date_trunc('month', "Date")
                ) as monthly_data
            """)
            
            last_date_query = text(f'SELECT MAX("Date") FROM {table}')

            with engine.connect() as conn:
                monthly_count = conn.execute(monthly_query).scalar()
                last_dt = conn.execute(last_date_query).scalar()

            
            # 1. 判定 Empty
            if monthly_count == 0:
                status = "❌ Empty"
                detail = "数据库内无任何历史数据"
            
            # 2. 判定 Stale (过期)
            elif last_dt:
                last_date = pd.to_datetime(last_dt)
                last_date_str = last_date.strftime('%Y-%m-%d')
                
                # 如果最新数据不是本月的，也不是上个月月底的，就算 Stale
                # 这里我们放宽到 35 天，如果超过 35 天没数据，说明漏掉了整整一个月
                days_diff = (today - last_date).days
                if days_diff > 35:
                    status = "⚠️ Stale"
                    detail = f"最新数据日期为 {last_date_str}，已缺失最近月份数据"
            
            # 3. 判定数据量是否足够
            if monthly_count < 12 and status == "✅ OK":
                status = "⚠️ Insufficient"
                detail = f"月度有效数据仅 {monthly_count} 条"

        except Exception as e:
            status = "🚨 Error"
            detail = str(e)

        results.append({
            "Ticker": table,
            "Status": status,
            "Last_Date": last_date_str,
            "Total_Monthly_Points": monthly_count,
            "Detail": detail
        })

    # --- 保存报告 ---
    df = pd.DataFrame(results)
    print(df['Status'].value_counts())
    
    df.to_csv('QC_Monthly_Logic_Report.csv', index=False)
    df_failed = df[df['Status'] != "✅ OK"]
    df_failed.to_csv('QC_Monthly_Issues.csv', index=False)
    
    print(f"\n✅ QC结束！")

if __name__ == '__main__':
    run_monthly_logic_qc()
