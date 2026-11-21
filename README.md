# Shopping Sessions

Compiled C++17 pipeline for the Builders Buildathon Shopping Sessions track. The native engine reads LF-delimited CSV, resolves identities, keeps the earliest event copy, and aggregates in shopper order. DuckDB writes the final Parquet files. Build with `make parser` or use the root Dockerfile. Without the compiled binary, the Python entry point uses the SQL implementation.

## Native checkpoint

The scored SQL baseline at `2d0c3f8` earned **37.43 s** in hidden practice. On the local Apple machine, three runs of the compiled path took 3.069 / 2.906 / 2.922 s for 20M events (seed 42) and 12.328 / 8.667 / 8.630 s for 60M (seed 7). Both complete tables matched the baseline at each scale in duplicate-sensitive comparisons. Local timings include process startup and Parquet close. Page caches were not reset. This candidate has no official score.

### Compact identity index

The next checkpoint stores up to 12 identifier bytes inline with an explicit length and compares longer identifiers in full. It groups campaign counters by day. The local comparison used the same two inputs and produced identical tables.

- 20M events: 2.939 s / 2.35 s / 2.352 s (median 2.352 s).
- 60M events: 8.324 s / 6.686 s / 6.689 s (median 6.689 s).

The benchmark reports the largest individual process RSS across the parent and its children. Whole-container memory also includes file cache and other simultaneous usage.

## SQL reference validation

- Official rules downloaded and read November 21, 2026.
- All 36 official correctness fixtures pass with DuckDB 1.4.2.
- Credit and assists use separate click relations: equal-time campaigns are preserved for assists.
- Official 1M-event seed-1 development answers match exactly: 117,038 user rows and 27,400 campaign rows.
- Independent Python oracle matches all fixtures and 10 generated seeds (10,000 events each), both original and reshuffled.
- Seven additional regressions pass, including equal-time campaign assists, empty outputs, invalid clock values, embedded CR identities, and long leading zeros.
- A generated 60M-event / 15M-click input completes locally with a 16GB engine limit; the repeatability report records the final timings. Linux amd64 container correctness checks pass in GitHub Actions.
- The public practice receipt for the baseline is BB-9C4B911D.

## Run

Use Python 3.12. Install `requirements.txt`, then:

```sh
make parser  # requires a C++17 compiler
python pipeline.py /path/to/data /path/to/output --scratch /path/to/scratch
```

Defaults: `/data`, `/output`, `/scratch`. Set `NCPU` (default 8) and `MEMORY_LIMIT` (default 16GB) as needed. The container also uses a 16GB engine limit, leaving headroom under the stated 24GB cap. Both output directories must be clean or contain only this program's `part-0.parquet`.

```sh
docker build --platform linux/amd64 -t shopping-sessions .
mkdir -p out scratch
# Ensure UID 1000 can write out/ and scratch/ before running.
docker run --rm --platform linux/amd64 --cpus=8 --memory=24g --memory-swap=24g \
  --pids-limit=4096 --network=none --read-only --user=1000:1000 \
  --tmpfs /tmp:rw,size=64m \
  -v "$PWD/data:/data:ro" -v "$PWD/out:/output" -v "$PWD/scratch:/scratch" shopping-sessions
```

This command describes the full contest-resource rehearsal. GitHub Actions has passed a smaller 1M, 2-CPU, 5GB container test. Pin to eight available CPUs using `--cpuset-cpus` on the target Linux host. Docker is not installed in the current development environment.

## Design

1. Strict CSV parsing preserves CR, spaces and case and drops malformed records.
2. Union-find resolves transitive links and includes IDs absent from events.
3. Event dedup keeps the complete earliest-ingest row before validation.
4. Integer millisecond timestamps preserve all boundary conditions.
5. Sessions span linked IDs and midnight, with gaps strictly greater than 30 minutes.
6. ASOF credit picks the latest eligible click, with byte-order tie breaking.
7. Assists use merged inclusive seven-day coverage intervals for every campaign.
8. Typed Parquet daily tables include assist-only campaign rows and null categories when appropriate.

The C++17 helper strictly validates and normalizes CSV with bounded streaming memory. DuckDB reads the normalized scratch file, which is deleted after ingestion. Python link processing and window-sort event dedup remain optimization opportunities. Without the helper, a slower SQL ingestion fallback is available; build the helper for scale runs. The independent oracle does not reuse production parsing or SQL transformations.

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

## Scale measurements

Local Apple-silicon measurements; these are not contest-hardware scores.

| Input | Version | Pipeline wall time | Peak reported RSS |
| --- | --- | --- | --- |
| 20M events, 5M clicks, seed 42 | SQL text ingestion | 100.5 s | not recorded |
| 20M events, 5M clicks, seed 42 | Streaming C++ CSV normalization | 15.1 s | 9.04 GB |
| 60M events, 15M clicks, seed 7 | Final baseline; median of 3 local runs | 52.315 s | 15.78 GB maximum |

The native parser reduced event ingestion from 85.6 s to 4.8 s. Both versions produced output; final-scale limits and hidden-data correctness require further checks. Three final 60M runs completed in 50.724, 54.909, and 52.315 seconds with a 16GB engine limit. All output invariants passed; see `benchmark-60m.json`. GitHub Actions runs official fixtures, differential cases, and the official 1M answer comparison inside a Linux amd64, read-only, network-isolated container. The CI runner uses 2 CPUs and a 5GB container cap, not the contest's full resources.

See [VALIDATION.md](VALIDATION.md) for exact evidence, download checksums, reproduction commands, known stress-test failures, and remaining contest checks.
