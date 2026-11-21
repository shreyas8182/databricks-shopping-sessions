FROM gcc:14-bookworm AS parser-build
WORKDIR /build
COPY parse_csv.cpp .
RUN g++ -std=c++17 -O3 -static-libstdc++ -static-libgcc parse_csv.cpp -o parse_csv

FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 NCPU=8 MEMORY_LIMIT=20GB TMPDIR=/scratch HOME=/scratch
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline.py .
COPY --from=parser-build /build/parse_csv .
USER 1000:1000
ENTRYPOINT ["python", "/app/pipeline.py"]
