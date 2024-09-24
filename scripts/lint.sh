#!/bin/bash

####################################################################################################
# Performs code linting and type checks. Fails if errors are found
####################################################################################################

set -e -o pipefail

echo "Running black"
black --diff --check src/

echo "Running pyright"
pyright .
