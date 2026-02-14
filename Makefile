.PHONY: generate-types generate-types-clean lint test

generate-types:
	python scripts/generate_types.py

generate-types-clean:
	python scripts/generate_types.py --clean

lint:
	black --check --diff ./src ./tests

test:
	pytest -m "not integration_vendor"
