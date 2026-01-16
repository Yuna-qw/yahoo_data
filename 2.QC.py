import os
import pandas as pd
import datetime
from sqlalchemy import create_engine, text

# 强制立即输出日志
import sys
def print_flush(*args, **kwargs):
    print(*args, **kwargs)
    sys.stdout.flush()
VERSION_TAG = "2026-01-16 18:10 第n版"
print_flush(f"📢 [DEBUG] 脚本版本: {VERSION_TAG}")

# --- 数据库配置 ---
DB_USER = "yu"
DB_PASSWORD = os.getenv('DB_PASSWORD', 'Yahoo1223')
DB_HOST = "pgm-7xvv5102g97m8i18ho.pg.rds.aliyuncs.com"
DB_PORT = "5432"
DB_NAME = "yahoo_stock_data"

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_timeout=30,
    connect_args={"connect_timeout": 10}
)

def get_last_month_last_day():
    """获取上个月最后一天的日期字符串 (YYYY-MM-DD)"""
    today = datetime.datetime.now()
    # 逻辑：本月1号减去1天
    first_day_this_month = today.replace(day=1)
    last_day_last_month = first_day_this_month - datetime.timedelta(days=1)
    return last_day_last_month.strftime('%Y-%m-%d')

def run_stable_qc():
    # 判定基准：上月最后一天
    target_date_str = get_last_month_last_day()
    print_flush(f"🚀 开始月度数据 QC...")
    print_flush(f"📅 判定基准日期: {target_date_str}")
    
    # 获取所有表名
    get_tables_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    with engine.connect() as conn:
        tables = [row[0] for row in conn.execute(text(get_tables_query)).fetchall()]
    
    total = len(tables)
    
    update_list = []    # 1. 有更新
    failed_list = []    # 2. 更新异常
    empty_list = []     # 3. 空表

    for i, table in enumerate(tables):
        try:
            # 锁定 date 列查询最新一条
            query = text(f'SELECT "date" FROM "{table}" ORDER BY "date" DESC LIMIT 1')
            with engine.connect() as conn:
                res = conn.execute(query).fetchone()
            
            if res and res[0]:
                raw_date = res[0]
                if isinstance(raw_date, (datetime.datetime, datetime.date)):
                    last_dt_str = raw_date.strftime('%Y-%m-%d')
                else:
                    # 如果是字符串，截取前10位 (2022-02-28)
                    last_dt_str = str(raw_date)[:10]
                
                # 对比逻辑
                if last_dt_str >= target_date_str:
                    update_list.append({"Ticker": table, "Last_Date": last_dt_str})
                else:
                    failed_list.append({"Ticker": table, "Last_Date": last_dt_str})
            else:
                empty_list.append({"Ticker": table, "Status": "Empty"})
        
        except Exception as e:
            failed_list.append({"Ticker": table, "Last_Date": "Error", "Detail": str(e)[:50]})

        if (i + 1) % 100 == 0:
            print_flush(f"⏳ 进度: {i + 1} / {total}")

    # --- 保存结果 ---
    pd.DataFrame(update_list).to_csv('QC_Update.csv', index=False)
    pd.DataFrame(failed_list).to_csv('QC_UpdateFailed.csv', index=False)
    pd.DataFrame(empty_list).to_csv('QC_Empty.csv', index=False)

    print_flush("\n" + "="*40)
    print_flush(f"📊 QC 最终统计结果:")
    print_flush(f"1. ✅ 有更新股票数: {len(update_list)}  -> 详见 QC_Update.csv")
    print_flush(f"2. ❌ 更新异常数: {len(failed_list)}  -> 详见 QC_UpdateFailed.csv")
    print_flush(f"3. 🕳️ 空表数量: {len(empty_list)}  -> 详见 QC_Empty.csv")
    print_flush("="*40)

if __name__ == '__main__':
    run_stable_qc()


