#!/bin/bash

####################################################################################################
# Runs the demo of naturla language geocoding.
####################################################################################################

set -e -o pipefail

if [[ -f .env ]]; then
    source .env
fi

PYTHONPATH=src streamlit run src/natural_language_geocoding_demo/app.py
