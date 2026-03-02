FROM python:3.10-slim
WORKDIR /app
COPY . .
RUN pip install pandas yfinance duckdb python-dateutil openpyxl
CMD ["python", "2.QC.py"]
