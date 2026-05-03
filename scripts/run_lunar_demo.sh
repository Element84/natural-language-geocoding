#!/bin/bash

####################################################################################################
# Runs the demo of lunar natural language geocoding.
####################################################################################################

set -e -o pipefail

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

PYTHONPATH=src streamlit run src/natural_language_geocoding_demo/lunar_app.py
