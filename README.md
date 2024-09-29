# natural-language-geocoding

Natural Language Geocoding implements an AI technique to convert natural language descriptions of spatial areas into polygons.

## TODOS

* Add integration tests
  * Can have them just run locally for now
* Add unit tests

## Installing and Using

```shell
pip install natural-language-geocoding
natural-language-geocoding init
```

Set the `NOMINATIM_USER_AGENT` environment variable to identify your application with the [Nominatim](https://nominatim.org/) API.


## Running the demo

There's a streamlit demo in `src/natural_language_geocoding_demo`. It can be run by following these steps.

1. Set up AWS credentials. The demo uses AWS Bedrock Anthropic Claude as the LLM. Valid AWS access keys to an account need to be present.
2. Follow steps for local development
3. Run `NOMINATIM_USER_AGENT=your-nominatim-user-agent scripts/run_demo.sh`
4. It will open in your browser


## Developing

1. Install python and [uv](https://github.com/astral-sh/uv)
2. Checkout the code
3. Run `scripts/recreate_venv.sh`
4. Run `PYTHONPATH=src python src/natural_language_geocoding/main.py init`
5. Run `pre-commit install` to install the pre commit checks
6. Make changes
7. Verify linting passes `scripts/lint.sh`
8. Commit and push your changes

## Attribution

### OpenStreetMap

This library uses the OpenStreetMap Nominatim API for geocoding. Users of this library must follow OpenStreetMap's [attribution guidelines](https://osmfoundation.org/wiki/Licence/Attribution_Guidelines)

Users must also conform to the [Nominatim Usage Policy](https://operations.osmfoundation.org/policies/nominatim/). The environment variable `NOMINATIM_USER_AGENT` should be set to identify your application.

### Natural Earth

This library uses coastlines from https://github.com/martynafford/natural-earth-geojson which is distributed under the CC0-1.0 license. These are downloaded when the project is initialized.
