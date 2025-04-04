#!/bin/bash

set -e -o pipefail

set -a
source .env
set +a

PYTHONPATH=src python -u src/natural_language_geocoding/geocode_index/ingesters/natural_earth.py
