import os
import pandas as pd
import datetime
from sqlalchemy import create_engine, text

print("📌 2.QC.py 脚本已启动！当前时间:", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# --- 1. 数据库配置 ---
DB_USER = "yu"
DB_PASSWORD = os.getenv('DB_PASSWORD', 'Yahoo1223')
DB_HOST = "pgm-7xvv5102g97m8i18ho.pg.rds.aliyuncs.com"
DB_PORT = "5432"
DB_NAME = "yahoo_stock_data"

# 优化：增加查询超时，避免单条查询卡死
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_timeout=30,
    connect_args={
        "connect_timeout": 10,  # 数据库连接超时 10 秒
        "options": "-c statement_timeout=5000"  # 单条 SQL 查询超时 5 秒
    }
)

def check_table_has_date_column(table_name):
    """检查表是否有日期列"""
    check_query = text(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' 
        AND (column_name = 'date')
    """)
    with engine.connect() as conn:
        res = conn.execute(check_query).fetchone()
    return res[0] if res else None

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
    
    # 2. 第二步：分批循环检查
    for i, table in enumerate(tables):
        print(f"🔍 正在检查第 {i+1}/{total} 张表: {table}")
        
        try:
            # 先检查表是否有日期列
            date_column = check_table_has_date_column(table)
            if not date_column:
                results.append({
                    "Ticker": table, 
                    "Status": "🚨 无日期列", 
                    "Last_Date": "N/A", 
                    "Check": "表中无 date/Date/trade_date 列"
                })
                continue
            
            # 用实际存在的日期列查询最后一条数据
            query = text(f'SELECT "{date_column}" FROM "{table}" ORDER BY "{date_column}" DESC LIMIT 1')
            with engine.connect() as conn:
                res = conn.execute(query).fetchone()
            
            if res:
                last_dt = res[0]
                last_dt_str = last_dt.strftime('%Y-%m-%d') if hasattr(last_dt, 'strftime') else str(last_dt)
                # 判定时间是否足够新
                is_stale = "❌ 旧数据" if last_dt_str < target_month else "✅ 最新"
                results.append({
                    "Ticker": table, 
                    "Status": "有数据", 
                    "Last_Date": last_dt_str, 
                    "Check": is_stale
                })
            else:
                results.append({
                    "Ticker": table, 
                    "Status": "❌ 空表", 
                    "Last_Date": "N/A", 
                    "Check": "需补下载"
                })
        
        except Exception as e:
            error_msg = str(e)[:100]  # 截断过长的报错信息
            results.append({
                "Ticker": table, 
                "Status": "🚨 报错", 
                "Last_Date": "Error", 
                "Check": error_msg
            })
            print(f"❌ 检查表 {table} 出错: {error_msg}")

        # 每隔 100 张表打印一次进度
        if (i + 1) % 100 == 0:
            print(f"⏳ 进度: {i + 1} / {total} (已完成 {(i+1)/total*100:.1f}%)")

    # 3. 保存结果
    df = pd.DataFrame(results)
    df.to_csv('QC_Full_Report.csv', index=False)
    
    # 筛选出需要关注的异常表
    df_issues = df[df['Check'] != "✅ 最新"]
    df_issues.to_csv('QC_Attention_Needed.csv', index=False)
    
    print("\n" + "="*30)
    print(f"🏁 QC 完毕！总表数: {total}")
    print(f"🚩 异常/过期表数: {len(df_issues)}")
    print("✅ 报告已生成: QC_Attention_Needed.csv")
    print("="*30)

if __name__ == '__main__':
    run_stable_qc()
