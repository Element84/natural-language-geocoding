#!/bin/bash

####################################################################################################
# Runs the tests.
#
# Testing against LLMs will use canned responses unless --use-real-bedrock-client is passed in which
# case the tests will be run against the real bedrock. It assumes AWS credentials have been
# configured in that case.
####################################################################################################

set -e -o pipefail

USE_REAL_BEDROCK_CLIENT=false
WATCH_MODE=false

EXTRA_ARGS=()

for arg in "$@"; do
  case $arg in
    --use-real-bedrock-client)
      USE_REAL_BEDROCK_CLIENT=true
      shift
      ;;
    --watch)
      WATCH_MODE=true
      shift
      ;;
    *)
      EXTRA_ARGS+=("$arg")
  esac
done

export USE_REAL_BEDROCK_CLIENT
export PYTHONPATH=src:tests

if [[ "$WATCH_MODE" = true ]]; then
  pytest-watch -- -vv -rA --log-cli-level=INFO "${EXTRA_ARGS[@]}"
else
  # -vv for verbose output
  # -rA to capture stdout output
  # --log-cli-level=INFO to capture INFO level logs
  pytest -vv -rA --log-cli-level=INFO "${EXTRA_ARGS[@]}"
fi
