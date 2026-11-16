import os

import pytest

import natural_language_geocoding
from natural_language_geocoding.models import (
    AnySpatialNodeType,
    Between,
    CoastOf,
    Difference,
    NamedPlace,
)

parse_spatial_node = natural_language_geocoding.parse_spatial_node_from_text

# Set to "true" to run tests that call the real AWS Bedrock runtime.
USE_REAL_BEDROCK_RUNTIME_CLIENT = os.getenv("USE_REAL_BEDROCK_RUNTIME_CLIENT") == "true"

NOVA_PRO = "bedrock:us.amazon.nova-pro-v1:0"


EXAMPLES: list[tuple[str, AnySpatialNodeType]] = [
    (
        "between Alpha and Bravo except for Charlie",
        Difference(
            child_node_1=Between(
                child_node_1=NamedPlace(name="Alpha"),
                child_node_2=NamedPlace(name="Bravo"),
            ),
            child_node_2=NamedPlace(name="Charlie"),
        ),
    ),
    ("the port of miami", CoastOf(child_node=NamedPlace(name="Miami Florida"))),
]


@pytest.mark.skipif(
    not USE_REAL_BEDROCK_RUNTIME_CLIENT, reason="Test is only run when using real bedrock clients"
)
@pytest.mark.parametrize(("example_pair"), EXAMPLES)
def test_nova_geocoding(example_pair: tuple[str, AnySpatialNodeType]) -> None:
    llm = NOVA_PRO
    query, expected_node = example_pair
    node = parse_spatial_node(llm, query)
    assert node.root.model_dump() == expected_node.model_dump()


@pytest.mark.skipif(
    not USE_REAL_BEDROCK_RUNTIME_CLIENT, reason="Test is only run when using real bedrock clients"
)
@pytest.mark.parametrize(("example_pair"), EXAMPLES)
def test_claude_geocoding(example_pair: tuple[str, AnySpatialNodeType]) -> None:
    llm = natural_language_geocoding.DEFAULT_MODEL
    query, expected_node = example_pair
    node = parse_spatial_node(llm, query)
    assert node.root.model_dump() == expected_node.model_dump()
