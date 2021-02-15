all: test

.PHONY: test test%

test:
	python -m unittest discover -s tests
