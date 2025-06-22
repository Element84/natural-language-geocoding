import pytest
from e84_geoai_common.llm.models import (
    CLAUDE_4_SONNET,
    BedrockClaudeLLM,
    BedrockNovaLLM,
)
from e84_geoai_common.llm.tests.mock_bedrock_runtime import USE_REAL_BEDROCK_RUNTIME_CLIENT

import natural_language_geocoding
from natural_language_geocoding.models import (
    AnySpatialNodeType,
    Between,
    CoastOf,
    Difference,
    NamedPlace,
)

parse_spatial_node = natural_language_geocoding.parse_spatial_node_from_text  # type: ignore[reportPrivateUsage]


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
    llm = BedrockNovaLLM()
    query, expected_node = example_pair
    node = parse_spatial_node(llm, query)
    assert node.root.model_dump() == expected_node.model_dump()


@pytest.mark.skipif(
    not USE_REAL_BEDROCK_RUNTIME_CLIENT, reason="Test is only run when using real bedrock clients"
)
@pytest.mark.parametrize(("example_pair"), EXAMPLES)
def test_claude_geocoding(example_pair: tuple[str, AnySpatialNodeType]) -> None:
    llm = BedrockClaudeLLM(CLAUDE_4_SONNET)
    query, expected_node = example_pair
    node = parse_spatial_node(llm, query)
    assert node.root.model_dump() == expected_node.model_dump()
