FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app

# 8000: 网页/API   7000: TCP 延迟代理监听（默认，可用 LISTEN_PORT 改）
EXPOSE 8000 7000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
