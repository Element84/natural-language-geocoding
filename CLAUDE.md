# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Environment is managed with `uv` (Python 3.12+).

- Set up / recreate venv: `scripts/recreate_venv.sh` (`uv sync --all-extras`)
- Download required Natural Earth coastline data (needed by coast-related nodes): `PYTHONPATH=src python src/natural_language_geocoding/main.py init`
- Lint and type check: `scripts/lint.sh` (runs `ruff check src/ tests/` then `pyright .`). CI runs this, so it must pass. Activate `.venv` first or pyright won't resolve installed packages.
- Run tests: `scripts/test.sh`. It sets `PYTHONPATH=src:tests` and forwards extra args to pytest:
  - Single test: `scripts/test.sh tests/natural_language_geocoding_tests/test_natural_language_geocoding.py::test_union`
  - Watch mode: `scripts/test.sh --watch`
  - Against real AWS Bedrock (needs AWS credentials and a region): `scripts/test.sh --use-real-client`. Without this flag, tests marked with `skipif(not USE_REAL_BEDROCK_RUNTIME_CLIENT)` are skipped.
  - Set `PYDANTIC_AI_NO_BANNER=1` to hide the Pydantic AI startup banner.
- Update dependencies: `scripts/refresh_requirements.sh` (upgrades `uv.lock` and regenerates `requirements.txt`)
- Run the Streamlit demo: `scripts/run_demo.sh`
- Run LLM evaluations: `scripts/run_evaluator.sh` (calls Bedrock, writes results under `temp/`)
- Populate OpenSearch, **in this order** (WOF data is used to compute hierarchies for later ingests): `scripts/ingest_wof_places.sh`, `scripts/ingest_ne_places.sh`, `scripts/ingest_composed_places.sh`. These scripts and the evaluator `source .env`.

## Code style

- Ruff uses `select = ['ALL']` with a line length of 100 and Google docstrings. Pyright runs in `strict` mode, with unknown types reported as errors. Write fully typed code and use targeted `# noqa:` comments, as the existing code does.
- Snippets for manual testing are kept as commented-out code at the bottom of some modules, under `# ruff: noqa: ERA001`.

## Architecture

The library turns natural language ("within 10 km of Baltimore, north of I-70") into a shapely geometry in two stages:

1. **LLM to spatial tree.** `parse_spatial_node_from_text` in `src/natural_language_geocoding/__init__.py` runs a [Pydantic AI](https://ai.pydantic.dev/) agent built by `create_spatial_node_agent`. It accepts any Pydantic AI model, a model name (default `DEFAULT_MODEL`, Claude Haiku on Bedrock), or a prebuilt agent. The LLM must return JSON matching the `SpatialNode` pydantic schema. Pydantic AI's structured output modes need an object schema and `SpatialNode` is a recursive union at the root, so the agent uses `TextOutput` with `_parse_spatial_node`, which strips code fences or surrounding prose and raises `ModelRetry` on invalid JSON. After `MAX_PARSE_ATTEMPTS` attempts Pydantic AI raises `UnexpectedModelBehavior`. The system prompt is built in `prompt.py`: the `prompt.md` template is filled with the guideline strings, the JSON schema generated from `SpatialNode`, and `EXAMPLES`. Changing the models therefore changes the prompt automatically. Guideline text in `prompt.py` is the main lever for improving LLM output.
2. **Tree to geometry.** `models.py` defines the node types (`NamedPlace`, `Buffer`, `Intersection`, `Union`, `Difference`, `Between`, `BorderOf`, `BorderBetween`, `CoastOf`, `OffTheCoastOf`, `DirectionalConstraint`, `DirectionalSubset`). They are combined in the `AnySpatialNodeType` union and wrapped in the `SpatialNode` root model. Each node's `to_geometry(place_lookup)` evaluates recursively. Only `NamedPlace` touches the `PlaceLookup`, and the other nodes do pure geometry operations (see `splitter.py` for compass subsets and `natural_earth.py` for coastlines). To add a node type, add the class, add it to `AnySpatialNodeType`, and usually add prompt guidance and eval examples too.

**Place lookup** (`place_lookup.py`): `PlaceLookup` is an ABC that maps a `PlaceSearchRequest` (name, place type, and `in_continent`/`in_country`/`in_region` context) to a geometry.
- `GeocodeIndexPlaceLookup` (`geocode_index/geocode_index_place_lookup.py`) is the recommended implementation and is backed by OpenSearch (the "Geocode Index"). It resolves continent, country, and region context to place IDs through `PlaceCache` (`geocode_index/hierachical_place_cache.py`). That cache is built by scrolling OpenSearch once and is then stored as JSON in `GEOCODE_INDEX_CACHE_DIR` (default `./temp`). Bump the cache filename version if its format changes.
- `NominatimAPI` (OpenStreetMap) is deprecated but is still the default when no lookup is passed to `extract_geometry_from_text`.
- Tests use `CannedPlaceLookup` (`src/natural_language_geocoding/tests/`, which ships in the package) and `tests/.../geocode_index/in_memory_geocode_index.py`.

**Geocode Index** (`geocode_index/`): `geoplace.py` holds the `GeoPlace`, `GeoPlaceType`, and `Hierarchy` models. `index.py` holds the OpenSearch index mapping and the `GeocodeIndex` client. `opensearch_utils.py` provides a small typed query DSL (`QueryDSL`, `IndexField`). `ingesters/` contains the Who's On First, Natural Earth, and composed-place ingesters (composed places such as "Iberian Peninsula" are unions of other places). `hierarchy_finder.py` assigns parent hierarchies during ingest. Connection settings come from the `GEOCODE_INDEX_HOST`, `GEOCODE_INDEX_PORT`, `GEOCODE_INDEX_REGION`, `GEOCODE_INDEX_USERNAME`, and `GEOCODE_INDEX_PASSWORD` environment variables (see `docs/sources.md`).

**Evaluation** (`eval/`): `evaluator.py` defines `ExampleEval` cases (user text and expected spatial node) and runs them against `DEFAULT_MODEL`. `evaluation_core.py` and `tree_distance.py` score the parsed tree against the expected tree using tree edit distance and produce markdown reports from `eval/templates/`.

**Testing LLM calls:** unit tests in `test_natural_language_geocoding.py` pass a Pydantic AI `FunctionModel` that returns canned text responses (see `make_llm`), then check the resulting geometry against `CannedPlaceLookup` geometries. Parsing with real Bedrock models (Claude and Nova) is covered in `test_parsing_with_different_llms.py`.

**Shared helpers** (`common/`): geometry, GeoJSON, debugging (folium map display), and util (`timed_function`, `singleline`, `get_env_var`, `unique_by`) helpers originally derived from `e84-geoai-common`.
