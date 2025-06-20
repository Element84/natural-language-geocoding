# natural-language-geocoding

Geocoding is the process of converting a place into the location of that place. Natural Language Geocoding can geocode natural language descriptions of places on the Earth into the geometry.

## Installing and Using

```shell
pip install natural-language-geocoding
natural-language-geocoding init
```

See documentation on how to configure and populate the OpenSearch cluster in [Sources](docs/sources.md).

## Running the demo

There's a streamlit demo in `src/natural_language_geocoding_demo`. It can be run by following these steps.

1. Set up AWS credentials. The demo uses AWS Bedrock Anthropic Claude as the LLM. Valid AWS access keys to an account need to be present.
2. Follow steps for local development
3. Create and populate an OpenSearch cluster following the instructions in [Sources](docs/sources.md).
4. Run `scripts/run_demo.sh`
5. It will open in your browser


## Developing

1. Install python and [uv](https://github.com/astral-sh/uv)
2. Checkout the code
3. Run `scripts/recreate_venv.sh`
4. Run `PYTHONPATH=src python src/natural_language_geocoding/main.py init`
5. Run `pre-commit install` to install the pre commit checks
6. Make changes
7. Verify linting passes `scripts/lint.sh`
8. Commit and push your changes

## Contributions

We are happy to take contributions! It is best to get in touch with the maintainers about larger features or design changes *before* starting the work, as it will make the process of accepting changes smoother.

## Contributor License Agreement (CLA)

Everyone who contributes code to natural language geocoding will be asked to sign a CLA, which is based off of the Apache CLA.

- Download a copy of **one of** the following from the `docs/cla` directory in this repository:

  - Individual Contributor (You're using your time): `2024_1_24-Natural-Language-Geocoding-Open-Source-Contributor-Agreement-Individual.pdf`
  - Corporate Contributor (You're using company time): `2024_1_24-Natural-Language-Geocoding-Open-Source-Contributor-Agreement-Corporate.pdf`

- Sign the CLA -- either physically on a printout or digitally using appropriate PDF software.

- Send the signed CLAs to Element 84 via **one of** the following methods:

  - Emailing the document to contracts@element84.com
  - Mailing a hardcopy to: ``Element 84, 210 N. Lee Street Suite 203 Alexandria, VA 22314, USA``.


## Attribution

See [Sources](docs/sources.md) for attribution and licensing information.
