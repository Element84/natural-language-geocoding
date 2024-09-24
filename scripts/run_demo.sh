#!/bin/bash

####################################################################################################
# Runs the demo of naturla language geocoding.
####################################################################################################

set -e -o pipefail

if [[ -f .env ]]; then
    source .env
fi

PYTHONPATH=src gradio src/natural_language_geocoding_demo/app.py
