PYTHON ?= python3
PYTHONPATH := $(CURDIR)/src

.PHONY: test bench check clean repl serve

test:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -W error -m unittest discover -s tests -v

bench:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) benchmarks/run_benchmark.py

check:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m compileall -q src tests benchmarks
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -W error -m unittest discover -s tests -v

repl:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pybitcask.cli --db ./bitcask_data shell

serve:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pybitcask.cli --db ./bitcask_data serve --host 127.0.0.1 --port 7379

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf build dist *.egg-info bitcask_data
