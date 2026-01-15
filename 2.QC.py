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

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}", pool_timeout=30)

def run_stable_qc():
    # 判定基准：本月1号
    target_month = datetime.datetime.now().replace(day=1).strftime('%Y-%m-%d')
    print(f"🚀 开始QC... 判定基准日期: {target_month}")
    
    # 1. 第一步：只拿表名
    get_tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    with engine.connect() as conn:
        tables = [row[0] for row in conn.execute(text(get_tables_query)).fetchall()]
    
    total = len(tables)
    print(f"✅ 成功获取 {total} 张表名单，开始逐一核对...")

    results = []
    
    # 2. 第二步：分批循环检查（增加打印，防止卡死）
    for i, table in enumerate(tables):
        try:
            # 只取最后一行日期，极速查询
            query = text(f'SELECT "Date" FROM "{table}" ORDER BY "Date" DESC LIMIT 1')
            with engine.connect() as conn:
                res = conn.execute(query).fetchone()
            
            if res:
                last_dt = res[0]
                last_dt_str = last_dt.strftime('%Y-%m-%d') if hasattr(last_dt, 'strftime') else str(last_dt)
                # 判定时间是否足够新
                is_stale = "❌ 旧数据" if last_dt_str < target_month else "✅ 最新"
                results.append({"Ticker": table, "Status": "有数据", "Last_Date": last_dt_str, "Check": is_stale})
            else:
                results.append({"Ticker": table, "Status": "❌ 空表", "Last_Date": "N/A", "Check": "需补下载"})
        
        except Exception as e:
            results.append({"Ticker": table, "Status": "🚨 报错", "Last_Date": "Error", "Check": str(e)})

        # 每隔 100 张表打印一次进度
        if (i + 1) % 100 == 0:
            print(f"⏳ 进度: {i + 1} / {total} (已完成 {(i+1)/total*100:.1f}%)")

    # 3. 保存结果
    df = pd.DataFrame(results)
    df.to_csv('QC_Full_Report.csv', index=False)
    
    # 筛选出需要关注的“空表”或“旧数据”
    df_issues = df[df['Check'] != "✅ 最新"]
    df_issues.to_csv('QC_Attention_Needed.csv', index=False)
    
    print("\n" + "="*30)
    print(f"🏁 QC 完毕！总表数: {total}")
    print(f"🚩 异常/过期表数: {len(df_issues)}")
    print("✅ 报告已生成: QC_Attention_Needed.csv")
    print("="*30)

if __name__ == '__main__':
    run_stable_qc()
