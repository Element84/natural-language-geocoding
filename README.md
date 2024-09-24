# natural-language-geocoding

Natural Language Geocoding implements an AI technique to convert natural language descriptions of spatial areas into polygons.

## TODOS

* Setup snyk
* Add integration tests
  * Can have them just run locally for now
* Add unit tests

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
4. Make changes
5. Verify linting passes `scripts/lint.sh`
