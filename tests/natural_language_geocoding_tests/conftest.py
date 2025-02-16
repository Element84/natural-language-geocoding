from e84_geoai_common.llm.tests.pydantic_compare import custom_assertrepr_compare

# Register custom assertion for pydantic models
pytest_assertrepr_compare = custom_assertrepr_compare
