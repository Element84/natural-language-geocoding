import pytest
from e84_geoai_common.llm.models import (
    CLAUDE_BEDROCK_MODEL_IDS,
    BedrockClaudeLLM,
    BedrockNovaLLM,
)
from e84_geoai_common.llm.tests.mock_bedrock import USE_REAL_BEDROCK_CLIENT

import natural_language_geocoding
from natural_language_geocoding.models import Between, Difference, NamedPlace

parse_spatial_node = natural_language_geocoding._parse_spatial_node_from_text  # type: ignore[reportPrivateUsage]


_TEXT = "between Alpha and Bravo except for Charlie"

_EXPECTED_SPATIAL_NODE = Difference(
    child_node_1=Between(
        child_node_1=NamedPlace(name="Alpha"),
        child_node_2=NamedPlace(name="Bravo"),
    ),
    child_node_2=NamedPlace(name="Charlie"),
)


@pytest.mark.skipif(
    not USE_REAL_BEDROCK_CLIENT, reason="Test is only run when using real bedrock clients"
)
def test_nova_geocoding() -> None:
    llm = BedrockNovaLLM()
    node = parse_spatial_node(llm, _TEXT)
    assert node.root == _EXPECTED_SPATIAL_NODE


@pytest.mark.skipif(
    not USE_REAL_BEDROCK_CLIENT, reason="Test is only run when using real bedrock clients"
)
def test_claude_geocoding() -> None:
    llm = BedrockClaudeLLM(CLAUDE_BEDROCK_MODEL_IDS["Claude 3.5 Sonnet"])
    node = parse_spatial_node(llm, _TEXT)
    assert node.root == _EXPECTED_SPATIAL_NODE
