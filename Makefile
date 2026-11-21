PYTHON ?= python
STARTER ?= vendor/starter-duckdb
GENERATOR ?= vendor/generator/target/release/generate
.PHONY: test fixtures differential dev check-dev

test:
	$(PYTHON) -m unittest -v test_boundaries
fixtures:
	SCRATCH_DIR=$(CURDIR)/scratch $(PYTHON) $(STARTER)/scripts/run_fixtures.py $(STARTER)/fixtures -- $(PYTHON) pipeline.py
differential:
	$(PYTHON) validate.py --fixtures $(STARTER)/fixtures --generator $(GENERATOR) --seeds 10 --events 10000
dev:
	$(GENERATOR) --seed 1 --events 1000000 --clicks 250000 --output data/dev
	$(PYTHON) pipeline.py data/dev out/dev --scratch scratch
check-dev:
	$(PYTHON) $(STARTER)/scripts/compare.py out/dev vendor/dev-answer
