import os
import time
from llama_index.core import VectorStoreIndex, Document, StorageContext, Settings, load_index_from_storage
from llama_index.embeddings.dashscope import DashScopeEmbedding
from typing import List

# 配置
DASH_SCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")
EMBEDDING_MODEL_NAME = "text-embedding-v2"
INDEX_PATH = "llama_index_stock_index"

BASE_FALLBACK_RULES = """
- **视图/表概览:** 数据库使用 **DuckDB**，包含 `stock_data` (每日数据) 和 `stock_monthly_change` (月度分析数据) 两个对象。
- **ABSOLUTE CRITICAL RULE (列名引用):** DuckDB 列名引用时，除非包含空格（如 "Adj Close"），否则**禁止**加双引号，且**禁止**将列名强转为小写。直接使用字段原名即可（如 `Ticker`, `Date`, `Monthly_Change_Pct`）。
- **月度查询规则:** 涉及月度涨跌幅/额必须使用 `stock_monthly_change` 视图。
- **时间序列规则:** 查询最近/最新数据时，必须使用 `ORDER BY [日期字段] DESC`。
"""

# 1. 知识源 (Documents)
def define_rag_documents() -> List[Document]:
    """针对细粒度 RAG 知识文档列表。"""
    documents = [
        # 数据库概览及字段
        Document(text="数据库运行在 DuckDB 引擎上。包含：`stock_data` 表和 `stock_monthly_change` 视图。"),
        Document(text="`stock_data` 字段：Ticker (TEXT), Date (DATE), Country (TEXT), Open (DOUBLE), High (DOUBLE), Low (DOUBLE), Close (DOUBLE), \"Adj Close\" (DOUBLE), Volume (BIGINT)。"),
        Document(text="`stock_monthly_change` 字段：Ticker (TEXT), Country (TEXT), Month_Start_Date (TIMESTAMP), Monthly_Close (REAL), Prev_Monthly_Close (REAL), Monthly_Change_Amt (REAL), Monthly_Change_Pct (REAL)。"),

        # 引用规则
        Document(text="**ABSOLUTE CRITICAL DUCKDB RULE (标识符):** 字段名引用必须保持一致。带空格的字段必须加双引号，如 `\"Adj Close\"`。普通字段如 `Ticker`, `Monthly_Change_Pct` 不建议加引号，且不需要强转小写。"),

        # 强制输出规则
        Document(text="**ABSOLUTE CRITICAL OUTPUT RULE:** 所有查询必须 SELECT `Ticker` 和相应的日期字段 (`Date` 或 `Month_Start_Date`)，否则图表模块会崩溃。"),
        Document(text="**ABSOLUTE CRITICAL OUTPUT FORMAT:** 仅输出 DuckDB SQL。严禁输出 Markdown 代码块外的任何文字或注释。"),

        # 关键联接规则
        Document(text="**ABSOLUTE CRITICAL DUCKDB JOIN:** 联接 `stock_data` (T1) 和 `stock_monthly_change` (T2) 时，日期对齐必须使用 `date_trunc('month', T1.Date) = date_trunc('month', T2.Month_Start_Date)`。"),

        # 日期过滤规则
        Document(text="**CRITICAL DUCKDB DATE RULE 1:** 过滤年份使用 `year(Date) = 2025` 或 `date_part('year', Date) = 2025`。"),
        Document(text="**CRITICAL DUCKDB DATE RULE 2:** 过滤月份使用 `month(Date) = 10` 或 `date_part('month', Date) = 10`。"),
        Document(text="**CRITICAL DUCKDB TIMESTAMP:** DuckDB 处理字符串日期非常智能，直接使用 `'2025-01-01'` 即可，不需要像 Postgres 那样加 `::timestamp`。"),

        # 联接粒度对齐
        Document(text="**ABSOLUTE CRITICAL RULE (月末对齐):** 联接两表取月度收盘价时，必须增加过滤条件：`T1.Date = (SELECT MAX(sd_sub.Date) FROM stock_data sd_sub WHERE sd_sub.Ticker = T1.Ticker AND date_trunc('month', sd_sub.Date) = date_trunc('month', T1.Date))`。"),

        # Top N 规则
        Document(text="**Top N 规则:** 获取最大值或排名时，使用 `ORDER BY [字段] DESC LIMIT N`。"),

        # 联接示例模板
        Document(text="**CRITICAL EXAMPLE (DuckDB 模板):** 查询 'PARA' 2025年每月收盘价及涨幅：`SELECT T1.Ticker, T1.Date, T1.Close, T2.Monthly_Change_Pct FROM stock_data AS T1 JOIN stock_monthly_change AS T2 ON T1.Ticker = T2.Ticker AND date_trunc('month', T1.Date) = date_trunc('month', T2.Month_Start_Date) WHERE T1.Ticker = 'PARA' AND year(T1.Date) = 2025 AND T1.Date = (SELECT MAX(Date) FROM stock_data sd_sub WHERE sd_sub.Ticker = T1.Ticker AND date_trunc('month', sd_sub.Date) = date_trunc('month', T1.Date)) ORDER BY T1.Date`。"),
    ]
    return documents


def setup_rag_index_llamaindex():
    """
    设置并持久化 LlamaIndex 索引。
    """
    new_documents = define_rag_documents()

    # 1. 初始化 LlamaIndex 嵌入模型
    try:
        embedding_model = DashScopeEmbedding(
            api_key=DASH_SCOPE_API_KEY,
            model_name=EMBEDDING_MODEL_NAME
        )
        Settings.embed_model = embedding_model
    except Exception as e:
        print(f"嵌入模型初始化失败，请检查配置和API Key: {e}")
        return

    # 2. 检查索引是否已存在，如果存在则直接加载或跳过创建
    index_storage_path = os.path.join(INDEX_PATH, 'docstore.json')

    if os.path.exists(index_storage_path):
        print(f"检测到现有索引 {INDEX_PATH}，正在加载...")
        try:
            # 使用 load_index_from_storage 加载索引
            storage_context = StorageContext.from_defaults(persist_dir=INDEX_PATH)
            index = load_index_from_storage(storage_context=storage_context)
            print(f"索引已成功加载。")
            return  # 加载成功则退出，不进行后续的创建和持久化

        except Exception as e:
            print(f"加载现有索引失败：{e} (将尝试新建索引)")

    # 3. 创建 LlamaIndex 索引
    print(f"未检测到现有索引或加载失败，正在创建新的 LlamaIndex 索引，包含 {len(new_documents)} 条文档...")

    index = VectorStoreIndex.from_documents(new_documents)

    # 4. 持久化 LlamaIndex 索引
    print(f"索引创建完成，正在持久化到 {INDEX_PATH}...")
    index.storage_context.persist(persist_dir=INDEX_PATH)


if __name__ == "__main__":
    start_time = time.time()
    setup_rag_index_llamaindex()
    end_time = time.time()
    print(f"\nRAG索引设置完成，总耗时: {end_time - start_time:.2f} 秒")
