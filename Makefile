STARTER ?= vendor/starter-duckdb
GENERATOR ?= vendor/generator/target/release/generate
.PHONY: test fixtures dev check-dev parser

NATIVE_FLAGS = -std=c++17 -O3 -pthread native_sweep.cpp
RUN_LINK_FLAGS = $(if $(filter Linux,$(shell uname -s)),-static-libstdc++ -static-libgcc)
parser:
	$(CXX) -std=c++17 -O2 $(RUN_LINK_FLAGS) run.cpp -o run
	$(CXX) $(NATIVE_FLAGS) -o native_sweep
ifeq ($(shell uname -s)-$(shell uname -m),Linux-x86_64)
	$(CXX) -mavx2 -mbmi -mbmi2 -mpopcnt $(NATIVE_FLAGS) -o native_sweep_avx2
endif
	$(CXX) -std=c++17 -O3 parse_csv.cpp -o parse_csv

test: fixtures

fixtures: parser check_native
	./check_native fixtures "$(STARTER)/fixtures" ./run

check_native: check_native.cpp
	$(CXX) -std=c++17 -O2 $< -o $@

dev: parser
	$(GENERATOR) --seed 1 --events 1000000 --clicks 250000 --output data/dev
	./run data/dev out/dev
check-dev: check_native
	./check_native compare out/dev vendor/dev-answer

.PHONY: benchmark
DATA ?= data/scale60m
OUTPUT ?= out/native-benchmark
RUNS ?= 3
benchmark: parser benchmark_native
	./benchmark_native ./run "$(DATA)" "$(OUTPUT)" "$(RUNS)"

benchmark_native: benchmark_native.cpp
	$(CXX) -std=c++17 -O2 $< -o $@
