# 1. 使用官方的 Python 镜像作为基础
FROM python:3.12-slim

# 2. 设置工作目录
WORKDIR /app

# 3. 把当前文件夹的所有代码复制到镜像里
COPY . /app

# 4. 在镜像里安装您需要的插件
RUN pip install --no-cache-dir pandas yfinance multitasking

# 5. 告诉镜像，启动的时候运行您的 QC 代码
CMD ["python", "2.QC.py"]
