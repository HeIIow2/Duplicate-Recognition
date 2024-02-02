#!/bin/bash

# increment version in pyproject.toml

# build package
python3 -m build --wheel

# upload to pypi
python3 -m twine upload dist/*
python3 -m twine upload --repository gitlab_wc_duplicates dist/*
