import os
import re
import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import duckdb
import streamlit as st

from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import DashScopeEmbeddings

# 基础配置与环境检查
st.set_page_config(page_title="AI 股票网页", layout="wide")
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")
LLM_MODEL_NAME = "qwen3.5-plus"
API_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DUCKDB_DB_NAME = 'yahoo_stock_data.duckdb'
INDEX_PATH = "llama_index_stock_index"


# 数据库与 RAG 核心逻辑类
class DBManager:
    def get_connection(self):
        return duckdb.connect(database=DUCKDB_DB_NAME)

    def execute_sql_and_fetch(self, query: str) -> pd.DataFrame:
        conn = self.get_connection()
        try:
            # 自动维护 stock_data 视图逻辑
            tables = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
            table_names = [t[0] for t in tables if t[0] not in ['stock_monthly_change', 'stock_data']]

            if table_names:
                sql_parts = []
                for name in table_names:
                    parts = name.split('_')
                    display_ticker = f"{''.join(parts[:-1])}.{parts[-1]}".upper() if '_' in name else name.upper()
                    sql_parts.append(
                        f"SELECT '{display_ticker}' as Ticker, CAST(date AS DATE) as Date, open as Open, high as High, low as Low, close as Close, adj_close as \"Adj Close\", volume as Volume FROM \"{name}\"")

                conn.execute(f"CREATE OR REPLACE VIEW stock_data AS {' UNION ALL '.join(sql_parts)}")

                # 创建分析视图
                conn.execute(f"""
                    CREATE OR REPLACE VIEW stock_monthly_change AS
                    SELECT *, 
                    LAG(Monthly_Close) OVER (PARTITION BY Ticker ORDER BY Month_Start_Date) as Prev_Monthly_Close,
                    ((Monthly_Close / NULLIF(LAG(Monthly_Close) OVER (PARTITION BY Ticker ORDER BY Month_Start_Date), 0)) - 1) * 100 as Monthly_Change_Pct
                    FROM (
                        SELECT Ticker, CAST(date_trunc('month', Date) AS DATE) as Month_Start_Date, LAST(Close) as Monthly_Close
                        FROM stock_data GROUP BY Ticker, Month_Start_Date
                    )
                """)

            df_result = conn.execute(query).fetchdf()
            return df_result.fillna(0)
        finally:
            conn.close()


db_manager = DBManager()


# 初始化 RAG
@st.cache_resource
def get_retriever():
    try:
        embeddings = DashScopeEmbeddings(model="text-embedding-v2", dashscope_api_key=DASHSCOPE_API_KEY)
        vector_store = FAISS.load_local(INDEX_PATH, embeddings, allow_dangerous_deserialization=True)
        return vector_store.as_retriever(search_kwargs={"k": 3})
    except:
        return None


retriever = get_retriever()

# LLM 初始化
llm = ChatOpenAI(model=LLM_MODEL_NAME, openai_api_base=API_BASE_URL, openai_api_key=DASHSCOPE_API_KEY, temperature=0.0)


# 辅助函数
def clean_sql_output(sql_text: str) -> str:
    sql_text = re.sub(r'```sql\s*|```', '', sql_text, flags=re.IGNORECASE).strip()
    return sql_text.replace('\n', ' ')


def generate_chart_image(query: str, df: pd.DataFrame):
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    fig, ax = plt.subplots(figsize=(10, 5))

    # 简单的日期列识别
    date_col = 'Month_Start_Date' if 'Month_Start_Date' in df.columns else ('Date' if 'Date' in df.columns else None)
    val_col = 'Monthly_Change_Pct' if 'Monthly_Change_Pct' in df.columns else (
        'Close' if 'Close' in df.columns else df.columns[-1])

    if date_col:
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.sort_values(date_col)
        if 'Ticker' in df.columns and df['Ticker'].nunique() > 1:
            for ticker, group in df.groupby('Ticker'):
                ax.plot(group[date_col], group[val_col], marker='o', label=ticker)
            ax.legend()
        else:
            ax.plot(df[date_col], df[val_col], marker='o', color='#1f77b4')

        ax.set_title(f"数据走势图: {val_col}")
        fig.autofmt_xdate()

        os.makedirs('chart', exist_ok=True)
        path = f"chart/web_chart_{int(time.time())}.png"
        plt.savefig(path)
        plt.close(fig)
        return path
    return None


# Streamlit UI 界面
st.title("🤖 AI 股票数据大屏")
st.markdown("---")

with st.sidebar:
    st.header("📊 系统状态")
    st.success(f"模型: {LLM_MODEL_NAME}")
    if retriever:
        st.info("✅ RAG 知识库已就绪")
    else:
        st.warning("⚠️ RAG 未加载（请先运行 setup）")

    st.markdown("---")
    st.write("📖 **常用指令示例：**")
    st.caption("1. 查询 '000001.SZ' 最近 6 个月涨跌幅")
    st.caption("2. 画出 'AAPL' 2025 年的股价走势图")
    st.caption("3. 对比 'AAPL' 和 'AA' 最近三月表现并画图")

user_input = st.text_input("💬 请输入您的股票查询指令：", placeholder="想查什么？直接告诉我...")

if st.button("开始分析", type="primary"):
    if user_input:
        with st.spinner('AI 正在构造 SQL 并调取数据...'):
            # 获取 RAG 上下文
            context = ""
            if retriever:
                docs = retriever.invoke(user_input)
                context = "\n".join([d.page_content for d in docs])

            # 生成 SQL
            prompt = f"You are a DuckDB expert. Table: stock_data, View: stock_monthly_change. Rules: Use UPPER(Ticker), use strftime for dates. Context: {context}. Question: {user_input}. SQL Query:"
            response = llm.invoke(prompt)
            sql = clean_sql_output(response.content)

            with st.expander("🛠️ 查看生成的后端 SQL"):
                st.code(sql, language="sql")

            # 执行并展示
            try:
                df_res = db_manager.execute_sql_and_fetch(sql)
                if df_res.empty:
                    st.warning("查询结果为空，请确认股票代码或日期范围。")
                else:
                    # 识别是否需要画图
                    chart_keywords = ["画图", "图表", "走势", "曲线", "对比", "plot", "chart"]
                    is_chart_needed = any(k in user_input for k in chart_keywords)

                    if is_chart_needed:
                        c1, c2 = st.columns([1, 1])
                        with c1:
                            st.subheader("📋 数据报表")
                            st.dataframe(df_res, use_container_width=True)
                        with c2:
                            st.subheader("📈 趋势分析")
                            img_path = generate_chart_image(user_input, df_res)
                            if img_path:
                                st.image(img_path)
                            else:
                                st.info("数据格式不足以绘图。")
                    else:
                        st.subheader("📋 查询结果数据")
                        st.dataframe(df_res, use_container_width=True)
                        st.info("💡 提示：在指令中加入“画图”可以查看趋势图。")
            except Exception as e:
                st.error(f"分析出错: {e}")
    else:
        st.warning("请输入指令。")
