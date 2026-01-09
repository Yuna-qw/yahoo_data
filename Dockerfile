FROM python:3.9-slim
WORKDIR /app
COPY . .
RUN pip install pandas yfinance multitasking openpyxl
CMD ["python", "2.QC.py"]
