"""Provides python functions for parsing text to extract geospatial areas."""

import logging

from e84_geoai_common.llm.core import LLM
from e84_geoai_common.llm.extraction import ExtractionError, extract_data_from_text
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.models import SpatialNode
from natural_language_geocoding.place_lookup import NominatimAPI, PlaceLookup
from natural_language_geocoding.prompt import SYSTEM_PROMPT

logger = logging.getLogger(__name__)

# Some models (e.g. Claude Sonnet 5.5) can't be forced to call the extraction tool and may reply
# with text instead. Claude structured output can't be used because SpatialNode is recursive.
MAX_PARSE_ATTEMPTS = 3


def parse_spatial_node_from_text(llm: LLM, text: str) -> SpatialNode:
    """Parses out the spatial node from text.

    Retries if the LLM doesn't return a valid spatial node, raising the last ExtractionError if
    every attempt fails.
    """
    for attempt in range(1, MAX_PARSE_ATTEMPTS + 1):
        try:
            return extract_data_from_text(
                llm=llm, model_type=SpatialNode, system_prompt=SYSTEM_PROMPT, user_prompt=text
            )
        except ExtractionError:
            if attempt == MAX_PARSE_ATTEMPTS:
                raise
            logger.warning(
                "Attempt %d of %d to parse spatial node failed", attempt, MAX_PARSE_ATTEMPTS
            )
    raise AssertionError("unreachable")


def extract_geometry_from_text(
    llm: LLM, text: str, place_lookup: PlaceLookup | None = None
) -> BaseGeometry:
    """Extracts a spatial area referenced in text as geometry."""
    place_lookup = place_lookup or NominatimAPI()
    spatial_node = parse_spatial_node_from_text(llm, text)
    logger.info("Found spatial node from text %s: %s", text, spatial_node.model_dump_json(indent=2))
    return spatial_node.to_geometry(place_lookup)


#########################
# Code for manual testing
# ruff: noqa: ERA001

# from e84_geoai_common.llm.models.claude import CLAUDE_5_5_SONNET, BedrockClaudeLLM
# llm = BedrockClaudeLLM(model_id=CLAUDE_5_5_SONNET)
# extract_geometry_from_text(llm, "Within 10 km of Baltimore")
