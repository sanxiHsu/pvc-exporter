# 使用官方的Python Alpine镜像作为基础镜像
FROM python:3.9-alpine

# 设置工作目录
WORKDIR /app

# 复制requirements.txt文件
COPY requirements.txt /app

# 安装Python依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用程序代码
COPY pvc-exporter.py /app

# 设置环境变量
ENV PYTHONUNBUFFERED=1

# 设置默认的CMD
CMD ["python3", "/app/pvc-exporter.py"]