# Usages:
#
# to install all dependencies:
# $ make init
#
# to run style check:
# $ make style

init:
	pip install -r requirements.txt
style:
	black -S . --exclude now/thirdparty/*
	flake8 .
	isort .
	pycln .