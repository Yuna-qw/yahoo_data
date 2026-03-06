import os
import time
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import DashScopeEmbeddings
from langchain_core.documents import Document

# 配置
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")
EMBEDDING_MODEL_NAME = "text-embedding-v2"
INDEX_PATH = "llama_index_stock_index"

def define_rag_documents():
    """定义知识库内容"""
    texts = [
        "数据库运行在 DuckDB 引擎上。包含：`stock_data` 表和 `stock_monthly_change` 视图。",
        "`stock_data` 字段：Ticker (TEXT), Date (DATE), Country (TEXT), Open (DOUBLE), High (DOUBLE), Low (DOUBLE), Close (DOUBLE), \"Adj Close\" (DOUBLE), Volume (BIGINT)。",
        "`stock_monthly_change` 字段：Ticker (TEXT), Country (TEXT), Month_Start_Date (DATE), Monthly_Close (REAL), Prev_Monthly_Close (REAL), Monthly_Change_Amt (REAL), Monthly_Change_Pct (REAL)。",
        "**ABSOLUTE CRITICAL DUCKDB RULE:** 字段名引用必须保持一致。带空格的字段如 \"Adj Close\" 必须加双引号。普通字段如 Ticker, Monthly_Change_Pct 不加引号，不转小写。",
        "**ABSOLUTE CRITICAL OUTPUT RULE:** 所有查询必须 SELECT `Ticker` 和相应的日期字段 (Date 或 Month_Start_Date)。",
        "**ABSOLUTE CRITICAL OUTPUT FORMAT:** 仅输出 DuckDB SQL。严禁输出 Markdown 代码块外的任何文字。",
        "**CRITICAL DUCKDB DATE RULE:** 过滤年份使用 year(Date) = 2025，过滤月份使用 month(Date) = 10。",
        "**CASE INSENSITIVITY:** 始终使用 UPPER(Ticker) = UPPER('000001.SZ') 进行过滤。",
        "**JOIN RULE:** 联接两表时使用 T1.Ticker = T2.Ticker AND date_trunc('month', T1.Date) = date_trunc('month', T2.Month_Start_Date)。"
    ]
    return [Document(page_content=t) for t in texts]

def setup_rag_index_langchain():
    """设置并持久化 FAISS 索引"""
    print("正在初始化千问 Embeddings...")
    embeddings = DashScopeEmbeddings(
        model=EMBEDDING_MODEL_NAME,
        dashscope_api_key=DASHSCOPE_API_KEY
    )

    documents = define_rag_documents()
    
    print(f"正在创建 FAISS 向量库，包含 {len(documents)} 条规则...")
    # 创建向量库
    vector_store = FAISS.from_documents(documents, embeddings)

    # 持久化
    print(f"正在保存 FAISS 索引到 {INDEX_PATH}...")
    vector_store.save_local(INDEX_PATH)
    print("保存成功！现在目录中应该包含 index.faiss 和 index.pkl 了。")

if __name__ == "__main__":
    if not DASHSCOPE_API_KEY:
        print("错误：请先设置 DASHSCOPE_API_KEY 环境变量")
    else:
        start_time = time.time()
        setup_rag_index_langchain()
        print(f"\nFAISS 索引设置完成，总耗时: {time.time() - start_time:.2f} 秒")
