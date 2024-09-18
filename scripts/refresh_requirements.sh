#!/bin/bash

####################################################################################################
# TODO
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
