import os
import pandas as pd
import datetime
import duckdb
import sys

# 强制立即输出日志
def print_flush(*args, **kwargs):
    print(*args, **kwargs)
    sys.stdout.flush()

VERSION_TAG = "2026-03-02 DuckDB版"
print_flush(f"📢 [DEBUG] 脚本版本: {VERSION_TAG}")

# DuckDB 配置
DB_PATH = "yahoo_stock_data.duckdb"
con = duckdb.connect(DB_PATH)

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
    print_flush(f"🚀 开始 DuckDB 本地数据 QC...")
    print_flush(f"📅 判定基准日期: {target_date_str}")
    
    # 1. 获取所有表名
    # DuckDB 获取所有表名的快捷命令
    try:
        tables_df = con.execute("SHOW TABLES").df()
        tables = tables_df['name'].tolist()
    except Exception as e:
        print_flush(f"❌ 获取表列表失败: {e}")
        return
    
    total = len(tables)
    print_flush(f"🔍 库中共有 {total} 张表")
    
    update_list = []    # 1. 有更新
    failed_list = []    # 2. 更新异常
    empty_list = []     # 3. 空表

    # 2. 遍历检查
    for i, table in enumerate(tables):
        try:
            # DuckDB 查询最新一条日期
            # 注意：DuckDB 默认表名不区分大小写，除非加双引号
            query = f'SELECT "date" FROM "{table}" ORDER BY "date" DESC LIMIT 1'
            res = con.execute(query).fetchone()
            
            if res and res[0]:
                raw_date = res[0]
                # DuckDB 返回的通常已经是 datetime 对象
                if isinstance(raw_date, (datetime.datetime, datetime.date)):
                    last_dt_str = raw_date.strftime('%Y-%m-%d')
                else:
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

        if (i + 1) % 100 == 0 or (i + 1) == total:
            print_flush(f"⏳ 进度: {i + 1} / {total}")

    # 3. 保存结果
    pd.DataFrame(update_list).to_csv('QC_Update.csv', index=False)
    pd.DataFrame(failed_list).to_csv('QC_UpdateFailed.csv', index=False)
    pd.DataFrame(empty_list).to_csv('QC_Empty.csv', index=False)

    print_flush("\n" + "="*40)
    print_flush(f"📊 QC 最终统计结果 (DuckDB):")
    print_flush(f"1. ✅ 有更新股票数: {len(update_list)}  -> QC_Update.csv")
    print_flush(f"2. ❌ 更新滞后/异常数: {len(failed_list)}  -> QC_UpdateFailed.csv")
    print_flush(f"3. 🕳️ 空表数量: {len(empty_list)}  -> QC_Empty.csv")
    print_flush("="*40)

if __name__ == '__main__':
    run_stable_qc()
