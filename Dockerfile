FROM python:3.10-slim
WORKDIR /app
COPY . .
RUN pip install pandas yfinance multitasking openpyxl sqlalchemy psycopg2-binary
CMD ["python", "2.QC.py"]
