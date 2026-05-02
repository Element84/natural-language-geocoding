#!/bin/bash

set -e -o pipefail

set -a
source .env
set +a

echo "Ingesting lunar places (will download from USGS if not cached)"

PYTHONPATH=src python -u -c "
from natural_language_geocoding.geocode_index.ingesters.lunar_nomenclature import ingest_lunar_places
ingest_lunar_places(recreate=True)
"
