PYTHON ?= python
STARTER ?= vendor/starter-duckdb
GENERATOR ?= vendor/generator/target/release/generate
.PHONY: test fixtures differential dev check-dev parser

NATIVE_FLAGS = -std=c++17 -O3 -pthread native_sweep.cpp
RUN_LINK_FLAGS = $(if $(filter Linux,$(shell uname -s)),-static-libstdc++ -static-libgcc)
parser:
	$(CXX) -std=c++17 -O2 $(RUN_LINK_FLAGS) run.cpp -o run
	$(CXX) $(NATIVE_FLAGS) $(RUN_LINK_FLAGS) -o native_sweep
ifeq ($(shell uname -s)-$(shell uname -m),Linux-x86_64)
	$(CXX) -mavx2 -mbmi -mbmi2 -mpopcnt $(NATIVE_FLAGS) $(RUN_LINK_FLAGS) -o native_sweep_avx2
endif
	$(CXX) -std=c++17 -O3 parse_csv.cpp -o parse_csv

test: parser
	$(PYTHON) -m unittest -v test_boundaries
fixtures: parser
	SCRATCH_DIR=$(CURDIR)/scratch $(PYTHON) $(STARTER)/scripts/run_fixtures.py $(STARTER)/fixtures -- ./run
differential: parser
	$(PYTHON) validate.py --fixtures $(STARTER)/fixtures --generator $(GENERATOR) --seeds 10 --events 10000
dev: parser
	$(GENERATOR) --seed 1 --events 1000000 --clicks 250000 --output data/dev
	./run data/dev out/dev
check-dev:
	$(PYTHON) $(STARTER)/scripts/compare.py out/dev vendor/dev-answer

.PHONY: benchmark
benchmark: parser
	$(PYTHON) benchmark.py data/scale60m out/scale60m --runs 3 --report benchmark-60m.json
