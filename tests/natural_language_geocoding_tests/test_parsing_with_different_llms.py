import pytest
from e84_geoai_common.llm.models.claude import CLAUDE_5_5_SONNET, BedrockClaudeLLM
from e84_geoai_common.llm.models.converse import BedrockConverseLLM
from e84_geoai_common.llm.models.nova import NOVA_PRO
from e84_geoai_common.llm.tests.mock_bedrock_runtime import USE_REAL_BEDROCK_RUNTIME_CLIENT

import natural_language_geocoding
from natural_language_geocoding.geocode_index.geoplace import GeoPlaceType
from natural_language_geocoding.models import (
    AnySpatialNodeType,
    Between,
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
    (
        "the port of miami",
        NamedPlace(
            name="Miami",
            type=GeoPlaceType.port,
            in_continent="North America",
            in_country="United States",
            in_region="Florida",
        ),
    ),
]


@pytest.mark.skipif(
    not USE_REAL_BEDROCK_RUNTIME_CLIENT, reason="Test is only run when using real bedrock clients"
)
@pytest.mark.parametrize(("example_pair"), EXAMPLES)
def test_nova_geocoding(example_pair: tuple[str, AnySpatialNodeType]) -> None:
    # BedrockNovaLLM doesn't support tool use, which extract_data_from_text requires.
    llm = BedrockConverseLLM(NOVA_PRO)
    query, expected_node = example_pair
    node = parse_spatial_node(llm, query)
    assert node.root.model_dump() == expected_node.model_dump()


@pytest.mark.skipif(
    not USE_REAL_BEDROCK_RUNTIME_CLIENT, reason="Test is only run when using real bedrock clients"
)
@pytest.mark.parametrize(("example_pair"), EXAMPLES)
def test_claude_geocoding(example_pair: tuple[str, AnySpatialNodeType]) -> None:
    llm = BedrockClaudeLLM(CLAUDE_5_5_SONNET)
    query, expected_node = example_pair
    node = parse_spatial_node(llm, query)
    assert node.root.model_dump() == expected_node.model_dump()
