# Validation record

Prepared November 21, 2026. Results below concern generated/public inputs, not hidden contest data.

## Correctness evidence

- All 36 official fixtures pass using the official Polars-based checker (exact columns, physical types, nulls, keys, and values).
- Official development set: seed 1, 1,000,000 events, 250,000 clicks. Both tables match the published answers exactly: 117,038 user rows and 27,400 campaign rows.
- Independent Python oracle matches the production output on all fixtures, 10 seeds with 10,000 events / 2,500 clicks each, and those same cases with records reversed and consolidated into one file per input kind.
- The independent oracle additionally matches the entire 1M development input.
- Focused regressions cover equal-time clicks from different campaigns, equal-time event peers following a session gap, impossible clock values, empty outputs, embedded CR in linked IDs, arbitrarily long leading-zero numbers, and invalid earliest duplicate copies.
- The production cumulative session counter uses a RANGE frame so equal-time peers share a session independent of their internal order.

## Reproduce the test materials

The downloads below are official challenge artifacts. Downloaded fixtures, answers, binaries, and generated inputs remain outside tracked source.

```sh
mkdir -p vendor
for name in starter-duckdb generator dev-answer; do
  curl --fail --location "https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/$name.zip" -o "vendor/$name.zip"
  unzip -q "vendor/$name.zip" -d vendor
done
cargo build --release --manifest-path vendor/generator/Cargo.toml
Download the DuckDB CLI for the host and place it on `PATH` for `check_native`.
make test fixtures differential dev check-dev
```

Checksums (SHA-256) of the artifacts actually tested:

| File | SHA-256 |
| --- | --- |
| starter-duckdb.zip | b84ff8726c43749043c33c250548a9ac9987b9132d28ac818a47515bd128f77c |
| generator.zip | 13724cadbac5a99d847e27cea911cd2e338ff972b46c2039da87452011bb101e |
| dev-answer.zip | 7eb8e1f0b1f431d178d28ac9437ce85809c44eb6393fdd42fb59857f30834fa9 |
| rules.md | 9dc9c600fb483292de92932ab30b0d40ec626b14df92777d84451c59cc2a2437 |

## Scale and environment limits

`benchmark_native` measures process start through exit. The native validator checks daily-key uniqueness, matching cross-table session/revenue totals, and the assist lower bound. These invariants do not prove every hidden answer correct.

```sh
vendor/generator/target/release/generate --seed 7 --events 60000000 --clicks 15000000 --output data/scale60m
make benchmark DATA=data/scale60m OUTPUT=out/scale60m RUNS=3
```

The local machine runs Apple-silicon native code with eight configured DuckDB threads; CPUs are not pinned and operating-system caches are not reset. These timings do not predict the contest's linux/amd64 score. A 10GB engine memory stress test failed in 60M deduplication; the first 16GB run completed in 54.3 seconds. Use the documented resource settings, and retest unseen seeds before submission.

GitHub CI builds the linux/amd64 image and compares its 1M outputs against the official answers. It runs with read-only root, UID 1000, no runtime network, 64MB /tmp, no swap allowance, and a 5GB container limit / 4GB engine limit on 2 CPUs. This validates container behavior at development scale. A full 60M container test on eight pinned x86 CPUs / 24GB remains outstanding.

## Remaining contest work

1. Run the exact candidate on hidden practice data and record its score.
2. Rehearse 60M on representative x86 Linux contest resources with additional seeds.
3. Verify image size, build duration, peak combined scratch/output storage and clean-checkout reproducibility on that host.
4. Confirm team eligibility/registration and repository visibility requirements. This repository was private during development; no visibility change or contest submission has been made.
5. Select the final entry explicitly before the verified contest deadline.

## Final repeated 60M results

Seed 7; 60,000,000 generated events and 15,000,000 generated clicks; eight threads; 16GB engine memory limit. Process wall times: **50.724, 54.909, 52.315 seconds**; median **52.315 seconds**. Maximum measured process RSS: **15.78 GB**. All three runs exited successfully and passed the recorded output invariants, with matching summary totals. The JSON report includes source hashes. These are repeated runs on the same generated seed, not three independent hidden datasets.

All runs produced 10,052,231 sessions, 58,688,623 surviving events, 20,262,829,952 cents revenue, 6,645,916 user daily rows, 28,014 campaign daily rows, and 106,596,029 campaign/session assists. The final output is about 49MB; transient scratch usage was not sampled continuously.
