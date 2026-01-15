SHELL=/bin/bash

dev_docs:	
	python3 -mwebbrowser http://127.0.0.1:8000/
	mkdocs serve --livereload

create_env:
	curl -LsSf https://astral.sh/uv/install.sh | sh
	uv venv -p python3.11
	uv sync --group docs;
	uv pip install -e .
	@echo ""
	@echo "Virtual environment created."
	@echo "Run this command to activate the virtual environment:"
	@echo "source .venv/bin/activate"

drop_env:
	rm -rf .venv

