import os
import re
import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import duckdb

from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import DashScopeEmbeddings

# 数据库配置 (DuckDB)
DUCKDB_DB_NAME = 'yahoo_stock_data.duckdb'
TABLE_NAME = 'stock_data'
VIEW_NAME = 'stock_monthly_change'

# LLM 和 RAG 配置
LLM_MODEL_NAME = "qwen3.5-plus" 
API_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")
INDEX_PATH = "llama_index_stock_index"


# 数据库连接和执行函数
class DBManager:
    """DuckDB 数据库连接管理和执行"""
    def get_connection(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(database=DUCKDB_DB_NAME)

    def execute_sql_and_fetch(self, query: str) -> pd.DataFrame:
        conn = self.get_connection()
        try:
            # 1. 获取数据库中所有的物理表名
            tables = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
            table_names = [t[0] for t in tables if t[0] not in [VIEW_NAME, 'stock_data']]

            if not table_names:
                raise Exception("数据库中没有任何股票数据表，请先运行下载脚本。")

            # 2. 动态创建一个大表 stock_data
            union_query = "CREATE OR REPLACE VIEW stock_data AS " + " UNION ALL ".join([
                f"SELECT UPPER('{name}') as Ticker, 'Global' as Country, CAST(date AS DATE) as Date, open as Open, high as High, low as Low, close as Close, adj_close as \"Adj Close\", volume as Volume FROM \"{name}\""
                for name in table_names
            ])
            conn.execute(union_query)

            # 3. 动态创建分析视图
            conn.execute(f"""
                CREATE OR REPLACE VIEW {VIEW_NAME} AS
                SELECT *, 
                LAG(Monthly_Close) OVER (PARTITION BY Ticker ORDER BY Month_Start_Date) as Prev_Monthly_Close,
                (Monthly_Close - LAG(Monthly_Close) OVER (PARTITION BY Ticker ORDER BY Month_Start_Date)) as Monthly_Change_Amt,
                ((Monthly_Close / NULLIF(LAG(Monthly_Close) OVER (PARTITION BY Ticker ORDER BY Month_Start_Date), 0)) - 1) * 100 as Monthly_Change_Pct
                FROM (
                    SELECT Ticker, Country, 
                           CAST(date_trunc('month', Date) AS DATE) as Month_Start_Date,
                           LAST(Close) as Monthly_Close
                    FROM stock_data
                    GROUP BY Ticker, Country, Month_Start_Date
                )
            """)

            # 3. 动态创建分析视图 stock_monthly_change
            conn.execute(f"""
                CREATE OR REPLACE VIEW {VIEW_NAME} AS
                SELECT *, 
                LAG(Monthly_Close) OVER (PARTITION BY Ticker ORDER BY Month_Start_Date) as Prev_Monthly_Close,
                (Monthly_Close - LAG(Monthly_Close) OVER (PARTITION BY Ticker ORDER BY Month_Start_Date)) as Monthly_Change_Amt,
                ((Monthly_Close / NULLIF(LAG(Monthly_Close) OVER (PARTITION BY Ticker ORDER BY Month_Start_Date), 0)) - 1) * 100 as Monthly_Change_Pct
                FROM (
                    SELECT Ticker, Country, 
                           date_trunc('month', CAST(Date AS DATE)) as Month_Start_Date,
                           LAST(Close) as Monthly_Close
                    FROM stock_data
                    GROUP BY Ticker, Country, Month_Start_Date
                )
            """)

            if not query.lower().strip().startswith("select"):
                raise ValueError("只允许执行 SELECT 查询。")
            df_result = conn.execute(query).fetchdf()
        except Exception as e:
            raise Exception(f"SQL执行失败: {e}")
        finally:
            conn.close()
        return df_result


db_manager = DBManager()

# LangChain 初始化
llm = ChatOpenAI(
    model=LLM_MODEL_NAME,
    openai_api_base=API_BASE_URL,
    openai_api_key=DASHSCOPE_API_KEY,
    temperature=0.0
)


class MockDuckDBSchema:
    """提供精确的 DuckDB 表和视图 Schema。"""

    def get_table_info(self):
        return (
            "Table stock_data has columns: Ticker (TEXT), Country (TEXT), Date (DATE), Open (DOUBLE), High (DOUBLE), Low (DOUBLE), Close (DOUBLE), \"Adj Close\" (DOUBLE), Volume (BIGINT)."
            "View stock_monthly_change has columns: Ticker (TEXT), Country (TEXT), Month_Start_Date (TEXT), Monthly_Close (REAL), Prev_Monthly_Close (REAL), Monthly_Change_Amt (REAL), Monthly_Change_Pct (REAL)."
        )


db_schema = MockDuckDBSchema()


# RAG 初始化
def initialize_retriever():
    """初始化 RAG 检索器"""
    try:
        embeddings = DashScopeEmbeddings(
            model="text-embedding-v2",
            dashscope_api_key=DASH_SCOPE_API_KEY
        )
        vector_store = FAISS.load_local(
            INDEX_PATH,
            embeddings,
            allow_dangerous_deserialization=True
        )
        print("Faiss 检索器已加载。RAG 已启用。")
        return vector_store.as_retriever(search_kwargs={"k": 3})
    except Exception:
        # 即使加载失败也继续，但 RAG 不起作用
        print("警告: Faiss 检索器加载失败或索引不存在。请先运行 rag_setup.py。")
        return None


retriever = initialize_retriever()

# Text-to-SQL Chain
SQL_PROMPT_TEMPLATE = """
You are a DuckDB expert. Given the table and view schemas, generate the best possible DuckDB query.

CRITICAL DUCKDB RULES:
1. DATE FILTERING: Never use 'LIKE' on DATE columns. 
   - For a specific month: Use "strftime(Month_Start_Date, '%Y-%m') = '2025-10'"
   - For a specific year: Use "EXTRACT(YEAR FROM Month_Start_Date) = 2025"
2. CASE INSENSITIVITY: Tickers might be stored in different cases. ALWAYS use "UPPER(Ticker) = UPPER('000001.SZ')" when filtering.
3. JOINING: If joining 'stock_data' and 'stock_monthly_change', join on UPPER(Ticker).
4. OUTPUT: Return ONLY the SQL query. No quotes, no explanations.

{table_info}
{rag_context}

Question: {question}
SQL Query:
"""

SQL_PROMPT = PromptTemplate(
    input_variables=["table_info", "question", "rag_context"],
    template=SQL_PROMPT_TEMPLATE,
)
sql_generation_chain = SQL_PROMPT | llm


def clean_sql_output(sql_text: str) -> str:
    """清理 LLM 生成的 SQL 语句。"""
    sql_text = re.sub(r'^\s*SQL\s*Query\s*:\s*', '', sql_text, flags=re.IGNORECASE).strip()
    sql_text = re.sub(r'```[sql]*\s*|```', '', sql_text, flags=re.IGNORECASE).strip()
    sql_text = re.sub(r'^\s*(\w+)\s+(SELECT|INSERT|UPDATE|DELETE|CREATE|DROP)', r'\2', sql_text,
                      flags=re.IGNORECASE).strip()
    sql_text = re.sub(r'\n', ' ', sql_text).strip()
    sql_text = sql_text.replace('`', '')
    return sql_text


def generate_chart_image(natural_language_query: str, df: pd.DataFrame) -> str:
    """
    使用 Matplotlib 根据查询类型和数据绘制图表。
    容 2 列时间序列、3 列及以上多系列时间序列以及非时间序列排名数据。
    """
    file_name_prefix = f"chart_{time.strftime('%Y%m%d%H%M%S')}"

    # 尝试从查询中提取股票代码用于图表标题
    ticker_match = re.search(r"'([A-Z0-9]+(?:\.[A-Z]+)?|\w+)'", natural_language_query)
    ticker_for_title = ticker_match.group(1) if ticker_match else "市场/多股"

    if df.empty:
        return "无法绘制图表: 数据结果集为空。"

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 6))

    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用于显示中文
    plt.rcParams['axes.unicode_minus'] = False

    is_time_series = any(name in df.columns for name in ['Date', 'Month_Start_Date'])

    if is_time_series and 'Ticker' in df.columns:
        # 情况 B: 多系列时间序列 (3列及以上)
        # 识别日期列、股票代码列和值列
        series_col = 'Ticker'
        potential_date_cols = [col for col in df.columns if col in ['Date', 'Month_Start_Date']]

        # 优先选择 Monthly_Change_Pct 或 Adj Close 作为值列
        y_col_candidate = [col for col in df.columns if 'Monthly_Change_Pct' in col]
        if not y_col_candidate:
            y_col_candidate = [col for col in df.columns if
                               'Adj Close' in col or 'Close' in col or 'Monthly_Close' in col]

        if not potential_date_cols or not y_col_candidate:
            return f"无法绘制图表: 无法从 {df.columns} 中识别 Ticker, Date/Month 和 Value 列。"

        x_col = potential_date_cols[0]
        y_col = y_col_candidate[0]

        df_plot = df[[series_col, x_col, y_col]].copy()

        try:
            df_plot[x_col] = pd.to_datetime(df_plot[x_col])
        except ValueError:
            return f"无法绘制图表: 日期列 '{x_col}' 转换失败，请检查数据格式。"

        df_plot[y_col] = pd.to_numeric(df_plot[y_col], errors='coerce')
        df_plot.dropna(subset=[y_col], inplace=True)

        for name, group in df_plot.groupby(series_col):
            ax.plot(group[x_col], group[y_col], marker='o', linestyle='-', label=name, linewidth=2)

        if 'Pct' in y_col:
            ax.axhline(0, color='grey', linestyle='--', linewidth=0.8)

        ax.set_title(f"股票指标对比 ({ticker_for_title})", fontsize=16)
        ax.set_xlabel(x_col)
        ax.set_ylabel(f'{y_col}')
        ax.legend(title=series_col)

        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        fig.autofmt_xdate(rotation=45)

        chart_type = "多股/多指标对比折线图"
        file_path = f"{file_name_prefix}_multi_line.png"

    elif is_time_series and len(df.columns) >= 2:
        # 标准单系列时间序列
        # 尝试识别日期列和值列
        date_cols = [col for col in df.columns if col in ['Date', 'Month_Start_Date']]
        value_cols = [col for col in df.columns if col not in date_cols and col != 'Ticker']

        if len(date_cols) != 1 or len(value_cols) != 1:
            # 默认取前两列，第一列非日期为值，第二列为日期
            potential_x = [col for col in df.columns if col in ['Date', 'Month_Start_Date']]
            potential_y = [col for col in df.columns if col not in potential_x]

            if len(potential_x) >= 1 and len(potential_y) >= 1:
                x_col = potential_x[0]
                y_col = potential_y[0]
            else:
                return f"无法绘制图表: 无法从 {df.columns} 中识别日期和值列。"
        else:
            x_col = date_cols[0]
            y_col = value_cols[0]

        df_plot = df[[x_col, y_col]].copy()

        try:
            df_plot[x_col] = pd.to_datetime(df_plot[x_col])
        except ValueError:
            return f"无法绘制图表: 日期列 '{x_col}' 转换失败，请检查数据格式。"

        df_plot[y_col] = pd.to_numeric(df_plot[y_col], errors='coerce')
        df_plot.dropna(subset=[y_col], inplace=True)

        ax.plot(df_plot[x_col], df_plot[y_col], marker='o', linestyle='-', color='#1f77b4', linewidth=2)

        if 'Pct' in y_col:
            ax.axhline(0, color='grey', linestyle='--', linewidth=0.8)

        ax.set_title(f"股票指标 ({ticker_for_title})", fontsize=16)
        ax.set_xlabel(x_col)
        y_label = f'{y_col} (%)' if 'Pct' in y_col else y_col
        ax.set_ylabel(y_label)

        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        fig.autofmt_xdate(rotation=45)

        chart_type = "折线图"
        file_path = f"{file_name_prefix}_line.png"

    elif len(df.columns) >= 2 and not is_time_series:
        # 情况 C: 排名/比较数据 (条形图)
        x_col = df.columns[0]
        y_col = df.columns[1]

        df[y_col] = pd.to_numeric(df[y_col], errors='coerce')
        df.dropna(subset=[y_col], inplace=True)

        df_sorted = df.sort_values(by=y_col, ascending=False)

        colors = ['red' if val > 0 else 'green' for val in df_sorted[y_col]]
        ax.bar(df_sorted[x_col], df_sorted[y_col], color=colors)

        if 'Pct' in y_col:
            ax.axhline(0, color='grey', linestyle='--', linewidth=0.8)

        ax.set_title("市场指标排名", fontsize=16)
        ax.set_xlabel(x_col)
        y_label = f'{y_col} (%)' if 'Pct' in y_col else y_col
        ax.set_ylabel(y_label)

        chart_type = "条形图"
        file_path = f"{file_name_prefix}_bar.png"

    else:
        return f"无法绘制图表: 数据格式不符合预期 (列数: {len(df.columns)})。"

    plt.tight_layout()
    os.makedirs('chart', exist_ok=True)
    full_path = os.path.join('chart', file_path)
    plt.savefig(full_path, dpi=300)
    plt.close(fig)

    return f"结果: {chart_type} 已成功生成！文件路径: {full_path}"


# 核心查询函数
def query_stock_data_with_llm(natural_language_query: str):
    try:
        print(f"\n原始查询: {natural_language_query}")
        start_query_time = time.time()
        rag_context = ""

        # 1. RAG 检索上下文
        if retriever:
            retrieved_docs = retriever.invoke(natural_language_query)
            context_list = [f"- {doc.page_content.replace('**', '')}" for doc in retrieved_docs]
            rag_context = "\n\nRAG 检索到的上下文 (k=3):\n" + "\n".join(context_list)
            print(rag_context)

        # 2. LLM 生成 SQL 语句
        llm_response_sql = sql_generation_chain.invoke(
            {
                "table_info": db_schema.get_table_info(),
                "question": natural_language_query,
                "rag_context": rag_context
            })
        sql_query = clean_sql_output(llm_response_sql.content)
        print(f"\n生成的 SQL:\n{sql_query}")

        # 3. 执行 SQL
        print("正在执行 SQL...")
        result_df = db_manager.execute_sql_and_fetch(sql_query)

        if result_df.empty:
            return "查询结果为空。"

        result_str = result_df.to_string(index=False)
        print(f"\n查询数据:\n{result_str}")

        # 4. 生成图表
        if "请生成" in natural_language_query or "画出" in natural_language_query or "图表" in natural_language_query:
            image_output = generate_chart_image(natural_language_query, result_df)
        else:
            image_output = "无需生成图表。"

        # 5. 整合最终输出
        final_output = (
            f"\n--- 结果 (总耗时 {time.time() - start_query_time:.2f}秒) ---\n"
            f"{image_output}\n"
        )
        return final_output

    except Exception as e:
        return f"查询失败! 错误信息: {e}"


if __name__ == "__main__":
    print(f"使用的 LLM 模型: {LLM_MODEL_NAME}")
    print(f"使用的数据库: DuckDB ({DUCKDB_DB_NAME})")

    print("\n" + "=" * 50 + "\n")

    query1 = "查询 '000001.SZ' 最近 6 个月的月度涨跌幅百分比 (Monthly_Change_Pct) 和日期 (Month_Start_Date)。"
    result1 = query_stock_data_with_llm(query1)
    print(result1)
    
    query2 = "在 'Shanghai_Shenzhen' 市场中，2025年10月月度涨幅百分比 (Monthly_Change_Pct) 第一是哪只股票？"
    result2 = query_stock_data_with_llm(query2)
    print(result2)

    query3 = "查询 'AA' 股票月度涨幅百分比 (Monthly_Change_Pct) 和日期 (Month_Start_Date) 的最近5条记录。"
    result3 = query_stock_data_with_llm(query3)
    print(result3)

    query4 = "关联stock_data和stock_monthly_change，查询 'AEM.TO' 2025 年的每月收盘价（Close）及该年的月度涨跌幅（Monthly_Change_Pct）"
    result4 = query_stock_data_with_llm(query4)
    print(result4)

    query5 = "查询 'AAP' 和 'AAPL' 最近 3 个月的月度涨跌幅（Monthly_Change_Pct），按日期和股票代码分组。"
    result5 = query_stock_data_with_llm(query5)
    print(result5)

    query6 = "查询 '000001.SZ' 近一年的最大收盘价 Close。"
    result6 = query_stock_data_with_llm(query6)
    print(result6)





