FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y libfreetype6-dev libpng-dev && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pandas \
    yfinance \
    duckdb \
    python-dateutil \
    openpyxl \
    llama-index-core \
    llama-index-embeddings-dashscope \
    llama-index-llms-openai \
    langchain-openai \
    langchain-community \
    faiss-cpu \
    matplotlib

COPY . .

CMD ["tail", "-f", "/dev/null"]
