#!/bin/bash

####################################################################################################
# Runs the demo of naturla language geocoding.
####################################################################################################

set -e -o pipefail

if [[ -f .env ]]; then
    source .env
fi

PYTHONPATH=src PYTHONPATH=src streamlit run streamlit_app.py
