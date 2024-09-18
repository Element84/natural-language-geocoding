#!/bin/bash

####################################################################################################
# TODO
####################################################################################################

set -e -o pipefail

uv pip compile \
  --refresh \
  --all-extras \
  pyproject.toml \
  dev-requirements.in \
  -o requirements.txt

uv pip install -r requirements.txt
