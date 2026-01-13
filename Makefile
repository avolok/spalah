SHELL=/bin/bash

dev_docs:	
	python3 -mwebbrowser http://127.0.0.1:8000/
	mkdocs serve --livereload

create_env:
	poetry env use python3.11	
	poetry lock
	poetry install --all-groups

drop_env:
	poetry env remove python3.11