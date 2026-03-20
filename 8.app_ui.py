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

st.set_page_config(page_title="AI股票查询网页", layout="wide")

# 1. 本地 Ollama 配置
LLM_MODEL_NAME = "llama3.2-vision"
API_BASE_URL = "http://localhost:11434/v1" 
LOCAL_OLLAMA_TOKEN = "ollama" 

# 2. 阿里云的 Key
ALIBABA_API_KEY = os.getenv("DASHSCOPE_API_KEY")

DUCKDB_DB_NAME = 'yahoo_stock_data.duckdb'
INDEX_PATH = "llama_index_stock_index"

# 历史记录初始化
if 'history' not in st.session_state:
    st.session_state['history'] = []

# 数据库管理类
class DBManager:
    def get_connection(self):
        # 增加 read_only=False 确保视图可以创建
        return duckdb.connect(database=DUCKDB_DB_NAME)

    def execute_sql_and_fetch(self, query: str) -> pd.DataFrame:
        conn = self.get_connection()
        try:
            # 自动化视图构建逻辑
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
            
            # 执行 AI 生成的查询
            return conn.execute(query).fetchdf().fillna(0)
        finally:
            conn.close()

db_manager = DBManager()

# RAG 与 LLM 初始化
@st.cache_resource
def get_retriever():
    try:
        embeddings = DashScopeEmbeddings(model="text-embedding-v2", dashscope_api_key=ALIBABA_API_KEY)
        vector_store = FAISS.load_local(INDEX_PATH, embeddings, allow_dangerous_deserialization=True)
        return vector_store.as_retriever(search_kwargs={"k": 3})
    except:
        return None

retriever = get_retriever()

llm = ChatOpenAI(
    model=LLM_MODEL_NAME, 
    openai_api_base=API_BASE_URL, 
    openai_api_key=LOCAL_OLLAMA_TOKEN, 
    temperature=0.0
)

def clean_sql_output(text: str) -> str:
    # 1. 移除 Markdown 代码块标记
    text = re.sub(r'```sql\s*|```', '', text, flags=re.IGNORECASE).strip()
    
    # 2. 移除常见的 AI 客套话前缀
    if "SELECT" in text.upper():
        text = text[text.upper().find("SELECT"):]
    
    # 3. 移除反引号和换行
    text = text.replace('`', '').replace('\n', ' ')
    
    # 4. 截断可能的解释文字（通常在分号后面）
    if ";" in text:
        text = text.split(";")[0] + ";"
        
    return text.strip()

def generate_chart_image(df: pd.DataFrame):
    try:
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'Tahoma', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
    except:
        pass
        
    fig, ax = plt.subplots(figsize=(10, 4))
    
    date_col = 'Month_Start_Date' if 'Month_Start_Date' in df.columns else ('Date' if 'Date' in df.columns else None)
    val_col = 'Monthly_Change_Pct' if 'Monthly_Change_Pct' in df.columns else ('Close' if 'Close' in df.columns else df.columns[-1])

    if date_col and not df.empty:
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.sort_values(date_col)
        
        if 'Ticker' in df.columns and df['Ticker'].nunique() > 1:
            for ticker, group in df.groupby('Ticker'):
                ax.plot(group[date_col], group[val_col], marker='o', label=ticker)
            ax.legend()
        else:
            ax.plot(df[date_col], df[val_col], marker='o', color='#1f77b4', linewidth=2)
        
        ax.set_title(f"Analysis Trend: {val_col}")
        ax.grid(True, linestyle='--', alpha=0.7)
        fig.autofmt_xdate()
        
        os.makedirs('chart', exist_ok=True)
        path = f"chart/web_chart_{int(time.time())}.png"
        plt.savefig(path, bbox_inches='tight')
        plt.close(fig)
        return path
    return None

# 界面布局
st.title("🤖 AI 股票数据分析系统")
st.markdown("---")

with st.sidebar:
    st.header("📊 运行状态")
    st.success(f"模式: 本地推理")
    st.info(f"模型: {LLM_MODEL_NAME}")
    if retriever: st.info("✅ RAG 已就绪")
    
    st.markdown("---")
    st.header("📜 最近查询历史")
    if not st.session_state['history']:
        st.info("暂无查询记录")
    else:
        for idx, item in enumerate(st.session_state['history']):
            with st.expander(f"🕒 {item['time']} - {item['query'][:10]}..."):
                st.write(f"**指令:** {item['query']}")
                if st.button("点此回溯结果", key=f"hist_{idx}"):
                    st.session_state['current_display'] = item
                    st.rerun()


# 主交互区
user_input = st.text_input("💬 请输入指令：", placeholder="例如：对比 AAPL 和 TSLA 最近三个月的收盘价走势...")

if st.button("开始执行", type="primary"):
    if user_input:
        with st.spinner('AI 正在构造 SQL 并检索数据库...'):
            try:
               final_prompt = f"""你是一个 DuckDB SQL 生成专家，你的任务是将用户需求精准转化为 SQL。
【数据库结构（Schema）】：
1. 表 `stock_data`（日线数据）：包含列 [Ticker, Date, Open, High, Low, Close, Volume]
2. 表 `stock_monthly_change`（月线指标）：包含列 [Ticker, Month_Start_Date, Monthly_Close, Monthly_Change_Pct]

【强制执行】：
1. 纯净输出：只输出 SQL，严禁任何解释性文字（如 "Here is..."），严禁 Markdown 符号。
2. 语法规范：严禁反引号 `。严禁 DATE_SUB()。
3. 时间计算：减去时间必须使用标准语法，例如 `CURRENT_DATE - INTERVAL '6 months'`。
4. **时间过滤（核心）**：如果用户提到“最近 X 个月/年”，必须在 WHERE 子句中包含日期限制。
   - 针对 `stock_monthly_change` 表，必须使用 `Month_Start_Date` 进行过滤。
   - 针对 `stock_data` 表，必须使用 `Date` 进行过滤。

【用户需求】：{user_input}"""

                response = llm.invoke(final_prompt)
                sql = clean_sql_output(response.content)

                df_res = db_manager.execute_sql_and_fetch(sql)
                
                chart_keywords = ["画图", "图表", "走势", "对比", "图", "plot", "chart"]
                is_chart_needed = any(k in user_input for k in chart_keywords)

                new_record = {
                    "time": time.strftime("%H:%M:%S"),
                    "query": user_input,
                    "sql": sql,
                    "data": df_res.copy(),
                    "has_chart": is_chart_needed
                }
                st.session_state['history'].insert(0, new_record)
                st.session_state['current_display'] = new_record
                st.rerun()

            except Exception as e:
                st.error(f"❌ 分析失败：{e}")
                if 'sql' in locals(): st.code(sql, language="sql")

# 结果展示
if 'current_display' in st.session_state:
    curr = st.session_state['current_display']
    st.markdown(f"#### 🔍 查询结果 ({curr['time']})")
    
    with st.expander("🛠️ 查看后端 SQL 指令"):
        st.code(curr['sql'], language="sql")
    
    if curr['data'].empty:
        st.warning("⚠️ 数据库中未找到符合条件的记录。")
    else:
        if curr['has_chart']:
            c1, c2 = st.columns([1, 1])
            with c1:
                st.dataframe(curr['data'], use_container_width=True)
            with c2:
                img_path = generate_chart_image(curr['data'])
                if img_path: st.image(img_path)
        else:
            st.dataframe(curr['data'], use_container_width=True)
