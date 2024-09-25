# natural-language-geocoding

Natural Language Geocoding implements an AI technique to convert natural language descriptions of spatial areas into polygons.

## TODOS

* Setup snyk
* Add integration tests
  * Can have them just run locally for now
* Add unit tests

## Installing

```shell
pip install natural-language-geocoding

# Download data files like coastlines.
natural-language-geocoding init
```


## Running the demo

There's a gradio demo in `src/natural_language_geocoding_demo`. It can be run by following these steps.

1. Set up AWS credentials. The demo uses AWS Bedrock Anthropic Claude as the LLM. Valid AWS access keys to an account need to be present.
2. Follow steps for local development
3. Run `scripts/run_demo.sh`
4. Open http://localhost:7860


## Developing

1. Install python and [uv](https://github.com/astral-sh/uv)
2. Checkout the code
3. Run `scripts/recreate_venv.sh`
4. Run `pre-commit install` to install the pre commit changes
5. Make changes
6. Verify linting passes `scripts/lint.sh`
7. Commit and push your changes
