SHELL=/bin/bash

dev_docs:	
	python3 -mwebbrowser http://127.0.0.1:8000/
	mkdocs serve --livereload
