FROM python:3.12-slim-bookworm AS libraries
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM libraries AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ make && rm -rf /var/lib/apt/lists/*
COPY native_sweep.cpp native_input.hpp parquet_output.hpp parse_csv.cpp Makefile ./
RUN make parser && mkdir /built && cp native_sweep parse_csv /built/ && if [ -f native_sweep_avx2 ]; then cp native_sweep_avx2 /built/; fi

FROM libraries
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 NCPU=8 MEMORY_LIMIT=16GB TMPDIR=/scratch HOME=/scratch
COPY pipeline.py native_output.py ./
COPY --from=build /built/ ./
USER 1000:1000
ENTRYPOINT ["python", "/app/pipeline.py"]
