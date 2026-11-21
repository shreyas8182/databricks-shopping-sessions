# Shopping Sessions

Exact-rule DuckDB baseline for the Builders Buildathon shopping-sessions track.

## Status

- Official rules downloaded and read November 21, 2026.
- All 36 official correctness fixtures pass with DuckDB 1.4.2.
- Credit and assists use separate click relations: equal-time campaigns are preserved for assists.
- Official 1M-event seed-1 development answers match exactly: 117,038 user rows and 27,400 campaign rows.
- Independent Python oracle matches all fixtures and 10 generated seeds (10,000 events each), both original and reshuffled.
- Six additional regressions pass, including equal-time campaign assists, empty outputs, invalid clock values, embedded CR identities, and long leading zeros.
- Full-scale and constrained container checks are still pending.
- No performance or submission-readiness claim is made yet.

## Run

Use Python 3.12. Install `requirements.txt`, then:

```sh
python pipeline.py /path/to/data /path/to/output --scratch /path/to/scratch
```

Defaults: `/data`, `/output`, `/scratch`. Set `NCPU` (default 8) and `MEMORY_LIMIT` (default 16GB) as needed. The container uses 20GB, leaving headroom under the stated 24GB cap. Both output directories must be clean or contain only this program's `part-0.parquet`.

```sh
docker build --platform linux/amd64 -t shopping-sessions .
mkdir -p out scratch
# Ensure UID 1000 can write out/ and scratch/ before running.
docker run --rm --platform linux/amd64 --cpus=8 --memory=24g --memory-swap=24g \
  --pids-limit=4096 --network=none --read-only --user=1000:1000 \
  --tmpfs /tmp:rw,size=64m \
  -v "$PWD/data:/data:ro" -v "$PWD/out:/output" -v "$PWD/scratch:/scratch" shopping-sessions
```

This command is a rehearsal recipe, not evidence of a completed container test. Pin to eight available CPUs using `--cpuset-cpus` on the target Linux host. Docker is not installed in the current development environment.

## Design

1. Strict CSV parsing preserves CR, spaces and case and drops malformed records.
2. Union-find resolves transitive links and includes IDs absent from events.
3. Event dedup keeps the complete earliest-ingest row before validation.
4. Integer millisecond timestamps preserve all boundary conditions.
5. Sessions span linked IDs and midnight, with gaps strictly greater than 30 minutes.
6. ASOF credit picks the latest eligible click, with byte-order tie breaking.
7. Assists use merged inclusive seven-day coverage intervals for every campaign.
8. Typed Parquet daily tables include assist-only campaign rows and null categories when appropriate.

Initial tradeoffs: whole-file text splitting, Python link processing, and window-sort event dedup prioritize a testable baseline. Profile and bound memory before final-scale use. The independent oracle must not reuse these SQL transformations.

## Validation

Download and extract the official DuckDB starter outside the tracked source tree. Its `scripts/run_fixtures.py` requires Polars (tested with 1.35.2):

```sh
SCRATCH_DIR="$PWD/scratch" python /path/to/starter-duckdb/scripts/run_fixtures.py \
  /path/to/starter-duckdb/fixtures -- python pipeline.py
```

Track: https://www.joinbuilders.org/buildathon/tracks/shopping-sessions

## Acknowledgments

Challenge specification, fixture inputs, expected answers, and starter guidance: Builders Buildathon Shopping Sessions track. Implementation uses DuckDB (MIT); the official fixture checker uses Polars (MIT). Python standard library is used for orchestration and link handling. AI assistance was used to develop and review this implementation. Downloaded datasets, reference answers, generator binaries, and local environments are excluded from the repository.

## Independent checks

```sh
python -m unittest -v test_boundaries
python validate.py --fixtures /path/to/starter-duckdb/fixtures \
  --generator /path/to/generate --seeds 10 --events 10000
```

`oracle.py` uses a separate byte-line parser, graph traversal, Python calendar validation, explicit session lists, and direct qualifying-click scans. DuckDB is used only to read the candidate Parquet in the comparison harness. The oracle is intended for small generated cases, not final-scale performance.

## Development history

The first fixture-passing commit exposed excessive memory use during the first 1M test. Replacing correlated line expansion with a projected expansion fixed it. A separate review also tightened clock validation so DuckDB cannot normalize hour 24 or second 60. The corrected 1M pipeline took about 2 seconds on the local Apple-silicon development machine; this is not a contest-hardware benchmark.
