#!/bin/bash

####################################################################################################
# Pulls downs the latest requirements as defined in the pyproject.toml and requirements.in files.
####################################################################################################

set -e -o pipefail

uv lock --upgrade
uv export --no-hashes --all-extras --format requirements-txt > requirements.txt
uv sync --all-extras
