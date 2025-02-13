#!/bin/bash

####################################################################################################
# Runs the tests.
#
# Testing against LLMs will use canned responses unless --use-real-bedrock-client is passed in which
# case the tests will be run against the real bedrock. It assumes AWS credentials have been
# configured in that case.
####################################################################################################

set -e -o pipefail

if [[ "$1" == "--use-real-bedrock-client" ]]; then
  export USE_REAL_BEDROCK_CLIENT=true
fi

PYTHONPATH=src:tests pytest
