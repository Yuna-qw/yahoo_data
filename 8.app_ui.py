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

# 1. 基础配置
st.set_page_config(page_title="AI股票查询网页", layout="wide")
DASHSCOPE_API_KEY = os.getenv("DASHSCOPE_API_KEY")
LLM_MODEL_NAME = "qwen3.5-plus"
API_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
DUCKDB_DB_NAME = 'yahoo_stock_data.duckdb'
INDEX_PATH = "llama_index_stock_index"

# 历史记录初始化
if 'history' not in st.session_state:
    st.session_state['history'] = []


# 数据库管理类
class DBManager:
    def get_connection(self):
        return duckdb.connect(database=DUCKDB_DB_NAME)

    def execute_sql_and_fetch(self, query: str) -> pd.DataFrame:
        conn = self.get_connection()
        try:
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
            return conn.execute(query).fetchdf().fillna(0)
        finally:
            conn.close()

db_manager = DBManager()

# RAG 与 LLM 初始化
@st.cache_resource
def get_retriever():
    try:
        embeddings = DashScopeEmbeddings(model="text-embedding-v2", dashscope_api_key=DASHSCOPE_API_KEY)
        vector_store = FAISS.load_local(INDEX_PATH, embeddings, allow_dangerous_deserialization=True)
        return vector_store.as_retriever(search_kwargs={"k": 3})
    except:
        return None


retriever = get_retriever()
llm = ChatOpenAI(model=LLM_MODEL_NAME, openai_api_base=API_BASE_URL, openai_api_key=DASHSCOPE_API_KEY, temperature=0.0)


def clean_sql_output(sql_text: str) -> str:
    sql_text = re.sub(r'```sql\s*|```', '', sql_text, flags=re.IGNORECASE).strip()
    return sql_text.replace('\n', ' ')


def generate_chart_image(df: pd.DataFrame):
    try:
        plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial Unicode MS', 'SimHei']
    except:
        pass
    plt.rcParams['axes.unicode_minus'] = False
    fig, ax = plt.subplots(figsize=(10, 4))
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
        ax.set_title(f"走势图: {val_col}")
        fig.autofmt_xdate()
        os.makedirs('chart', exist_ok=True)
        path = f"chart/web_chart_{int(time.time())}.png"
        plt.savefig(path)
        plt.close(fig)
        return path
    return None


# 界面布局
st.title("🤖 AI 股票数据查询")

# 侧边栏记录展示
with st.sidebar:
    st.header("📊 系统状态")
    if retriever:
        st.success("✅ RAG 知识库已就绪")
    else:
        st.warning("⚠️ RAG 未加载")

    st.markdown("---")
    st.header("📜 最近查询历史")
    if not st.session_state['history']:
        st.info("暂无查询记录")
    else:
        for idx, item in enumerate(st.session_state['history']):
            # 用小卡片形式展示
            with st.expander(f"🕒 {item['time']} - {item['query'][:10]}..."):
                st.write(f"**指令:** {item['query']}")
                st.code(item['sql'], language="sql")
                if st.button("查看此结果", key=f"hist_{idx}"):
                    st.info("此功能可配合方案三永久存储实现，当前仅供回顾。")

# 5. 主交互区
user_input = st.text_input("💬 请输入查询指令：", placeholder="例如：查询 AAPL 最近六个月涨跌幅并画图")

if st.button("开始分析", type="primary"):
    if user_input:
        with st.spinner('思考中...'):
            try:
                # RAG 上下文
                context = ""
                if retriever:
                    docs = retriever.invoke(user_input)
                    context = "\n".join([d.page_content for d in docs])

                # 生成 SQL
                prompt = f"You are a DuckDB expert. Table: stock_data, View: stock_monthly_change. Rules: Use UPPER(Ticker). Context: {context}. Question: {user_input}. SQL Query:"
                response = llm.invoke(prompt)
                sql = clean_sql_output(response.content)

                # 执行数据
                df_res = db_manager.execute_sql_and_fetch(sql)

                # 存入历史记录
                new_record = {
                    "time": time.strftime("%H:%M:%S"),
                    "query": user_input,
                    "sql": sql,
                    "data_count": len(df_res)
                }
                st.session_state['history'].insert(0, new_record)  # 最新的放上面

                # 展示结果
                with st.expander("🛠️ 查看SQL语句"):
                    st.code(sql, language="sql")

                if df_res.empty:
                    st.warning("未找到匹配数据。")
                else:
                    chart_keywords = ["画图", "图表", "走势", "对比", "plot", "chart"]
                    is_chart_needed = any(k in user_input for k in chart_keywords)

                    if is_chart_needed:
                        col1, col2 = st.columns(2)
                        with col1:
                            st.subheader("📋 数据报表")
                            st.dataframe(df_res, use_container_width=True)
                        with col2:
                            st.subheader("📈 趋势分析")
                            img = generate_chart_image(df_res)
                            if img: st.image(img)
                    else:
                        st.subheader("📋 查询结果")
                        st.dataframe(df_res, use_container_width=True)

                st.rerun()

            except Exception as e:
                st.error(f"分析失败: {e}")
    else:
        st.warning("请输入指令。")
