PYTHON ?= python
STARTER ?= vendor/starter-duckdb
GENERATOR ?= vendor/generator/target/release/generate
.PHONY: test fixtures differential dev check-dev parser

ARROW_DIR = $(shell $(PYTHON) -c "from importlib.util import find_spec; from pathlib import Path; print(Path(find_spec('pyarrow').origin).parent)")
ARROW_SUFFIX = $(if $(filter Darwin,$(shell uname)),.2200.dylib,.so.2200)

parser:
	$(CXX) -std=c++17 -O3 -pthread -I"$(ARROW_DIR)/include" native_sweep.cpp "$(ARROW_DIR)/libparquet$(ARROW_SUFFIX)" "$(ARROW_DIR)/libarrow$(ARROW_SUFFIX)" -Wl,-rpath,"$(ARROW_DIR)" -o native_sweep
	$(CXX) -std=c++17 -O3 parse_csv.cpp -o parse_csv

test: parser
	$(PYTHON) -m unittest -v test_boundaries
fixtures: parser
	SCRATCH_DIR=$(CURDIR)/scratch $(PYTHON) $(STARTER)/scripts/run_fixtures.py $(STARTER)/fixtures -- $(PYTHON) pipeline.py
differential: parser
	$(PYTHON) validate.py --fixtures $(STARTER)/fixtures --generator $(GENERATOR) --seeds 10 --events 10000
dev: parser
	$(GENERATOR) --seed 1 --events 1000000 --clicks 250000 --output data/dev
	$(PYTHON) pipeline.py data/dev out/dev --scratch scratch
check-dev:
	$(PYTHON) $(STARTER)/scripts/compare.py out/dev vendor/dev-answer

.PHONY: benchmark
benchmark: parser
	$(PYTHON) benchmark.py data/scale60m out/scale60m --runs 3 --report benchmark-60m.json
