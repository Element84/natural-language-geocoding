#!/bin/bash

####################################################################################################
# Pulls downs the latest requirements as defined in the pyproject.toml and requirements.in files.
####################################################################################################

set -e -o pipefail

rm -rf .venv

uv pip compile \
  --refresh \
  --all-extras \
  pyproject.toml \
  dev-requirements.in \
  -o requirements.txt

uv venv

uv pip install -r requirements.txt
