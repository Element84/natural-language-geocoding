#!/bin/bash

####################################################################################################
# Performs code linting and type checks. Fails if errors are found
####################################################################################################

set -e -o pipefail

echo "Running Ruff"
ruff check src/ tests/

echo "Running pyright"
pyright .
