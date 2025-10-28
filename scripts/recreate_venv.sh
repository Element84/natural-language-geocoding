#!/bin/bash

####################################################################################################
# Recreates the virtual environment with frozen dependencies.
####################################################################################################

set -e -o pipefail

rm -rf .venv || true
uv sync --all-extras
