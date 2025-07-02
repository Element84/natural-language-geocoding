"""Provides python functions for parsing text to extract geospatial areas."""

import logging

from e84_geoai_common.llm.core import LLM
from e84_geoai_common.llm.extraction import extract_data_from_text
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.models import SpatialNode
from natural_language_geocoding.place_lookup import NominatimAPI, PlaceLookup
from natural_language_geocoding.prompt import SYSTEM_PROMPT

logger = logging.getLogger(__name__)


def parse_spatial_node_from_text(llm: LLM, text: str) -> SpatialNode:
    """Parses out the spatial node from text."""
    return extract_data_from_text(
        llm=llm, model_type=SpatialNode, system_prompt=SYSTEM_PROMPT, user_prompt=text
    )


def extract_geometry_from_text(
    llm: LLM, text: str, place_lookup: PlaceLookup | None = None
) -> BaseGeometry:
    """Extracts a spatial area referenced in text as geometry."""
    place_lookup = place_lookup or NominatimAPI()
    spatial_node = parse_spatial_node_from_text(llm, text)
    logger.info("Found spatial node from text %s: %s", text, spatial_node.model_dump_json(indent=2))
    if g := spatial_node.to_geometry(place_lookup):
        return g
    raise Exception(f"Unable to extract geometry from text: {text}")


#########################
# Code for manual testing
# ruff: noqa: ERA001

# from e84_geoai_common.llm.core import BedrockClaudeLLM
# llm = BedrockClaudeLLM(model_id=CLAUDE_4_SONNET)
# extract_geometry_from_text(llm, "Within 10 km of Baltimore")
