FROM debian:bookworm-slim AS build
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends g++ make && rm -rf /var/lib/apt/lists/*
COPY native_sweep.cpp native_input.hpp parquet_output.hpp parquet_min.hpp parse_csv.cpp run.cpp Makefile ./
RUN make parser && mkdir /built && cp run native_sweep /built/ && if [ -f native_sweep_avx2 ]; then cp native_sweep_avx2 /built/; fi

FROM debian:bookworm-slim
WORKDIR /app
ENV NCPU=8 TMPDIR=/scratch HOME=/scratch
COPY --from=build /built/ ./
USER 1000:1000
ENTRYPOINT ["/app/run"]
