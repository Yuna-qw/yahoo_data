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

def run_super_fast_qc():
    print(f"🚀 启动超级闪电 QC... 当前时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 核心优化：直接从 PostgreSQL 系统统计表中一次性捞出所有表名和行数
    query = """
    SELECT 
        relname as table_name, 
        n_live_tup as row_count
    FROM pg_stat_user_tables 
    WHERE schemaname = 'public'
    ORDER BY n_live_tup DESC;
    """
    
    try:
        with engine.connect() as conn:
            df_all = pd.read_sql(text(query), conn)
        
        total_tables = len(df_all)
        print(f"统计到数据库内共有 {total_tables} 张表。")

        # 判定逻辑
        # ✅ OK: 行数 > 0
        # ❌ Empty: 行数 = 0
        df_all['Status'] = df_all['row_count'].apply(lambda x: "✅ OK" if x > 0 else "❌ Empty")
        
        # 筛选出有问题的表
        df_issues = df_all[df_all['Status'] == "❌ Empty"]
        
        # 保存报告
        df_all.to_csv('QC_Full_Inventory.csv', index=False)
        df_issues.to_csv('QC_Issues_Only.csv', index=False)
        
        print("-" * 30)
        print(f"📊 QC 报告汇总:")
        print(f"正常表数量: {total_tables - len(df_issues)}")
        print(f"异常(空表): {len(df_issues)}")
        print("-" * 30)
        print("✅ 报告已生成: QC_Full_Inventory.csv 和 QC_Issues_Only.csv")

    except Exception as e:
        print(f"🚨 QC 运行出错: {e}")

if __name__ == '__main__':
    run_super_fast_qc()
