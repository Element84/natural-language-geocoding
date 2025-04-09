#!/bin/bash

set -e -o pipefail

set -a
source .env
set +a

PYTHONPATH=src python -u src/natural_language_geocoding/eval/evaluator.py
