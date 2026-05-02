# === A股量化交易系统 — Python 后端 ===
FROM python:3.10-slim AS backend

WORKDIR /app

# 安装系统依赖（ClickHouse 客户端、psycopg2 等需要）
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目代码
COPY . .

# 默认启动 FastAPI
EXPOSE 8787
CMD ["uvicorn", "ui.server.app:app", "--host", "0.0.0.0", "--port", "8787"]
