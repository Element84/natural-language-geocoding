#!/bin/bash

####################################################################################################
# Runs automated evaluations using an LLM. See the evaluator for details.
####################################################################################################


set -e -o pipefail

set -a
source .env
set +a

PYTHONPATH=src python -u src/natural_language_geocoding/eval/evaluator.py
