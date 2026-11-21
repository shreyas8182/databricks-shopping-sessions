FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 NCPU=8 MEMORY_LIMIT=20GB TMPDIR=/scratch HOME=/scratch
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline.py .
USER 1000:1000
ENTRYPOINT ["python", "/app/pipeline.py"]
