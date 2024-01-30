#!/bin/bash

# increment version in pyproject.toml

# build package
python3 -m build --wheel

# upload to pypi
python3 -m twine upload dist/*
