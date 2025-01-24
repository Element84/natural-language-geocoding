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

# Contributions

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

### OpenStreetMap

This library uses the OpenStreetMap Nominatim API for geocoding. Users of this library must follow OpenStreetMap's [attribution guidelines](https://osmfoundation.org/wiki/Licence/Attribution_Guidelines)

Users must also conform to the [Nominatim Usage Policy](https://operations.osmfoundation.org/policies/nominatim/). The environment variable `NOMINATIM_USER_AGENT` should be set to identify your application.

### Natural Earth

This library uses coastlines from https://github.com/martynafford/natural-earth-geojson which is distributed under the CC0-1.0 license. These are downloaded when the project is initialized.
