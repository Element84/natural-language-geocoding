"""Provides python functions for parsing text to extract geospatial areas."""

import logging
import re

from pydantic import ValidationError
from pydantic_ai import Agent, ModelRetry, TextOutput
from pydantic_ai.models import KnownModelName, Model
from pydantic_ai.settings import ModelSettings
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.models import SpatialNode
from natural_language_geocoding.place_lookup import NominatimAPI, PlaceLookup
from natural_language_geocoding.prompt import SYSTEM_PROMPT

logger = logging.getLogger(__name__)

type LLM = Model | KnownModelName | str
"""A Pydantic AI model instance or a model name like DEFAULT_MODEL."""

DEFAULT_MODEL = "bedrock:us.anthropic.claude-haiku-5-5"

DEFAULT_MODEL_SETTINGS = ModelSettings(max_tokens=1000, temperature=0)

_JSON_CODE_FENCE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)


def _parse_spatial_node(text: str) -> SpatialNode:
    """Parses the LLM's text response into a SpatialNode, asking the LLM to retry if invalid."""
    if match := _JSON_CODE_FENCE.search(text):
        text = match.group(1)
    else:
        # Tolerate any prose before or after the JSON object
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end > start:
            text = text[start : end + 1]
    try:
        return SpatialNode.model_validate_json(text)
    except ValidationError as e:
        logger.warning("Unable to parse LLM response: %s", text)
        raise ModelRetry(f"The response was not valid JSON matching the schema: {e}") from e


def create_spatial_node_agent(
    llm: LLM = DEFAULT_MODEL, model_settings: ModelSettings | None = None
) -> Agent[None, SpatialNode]:
    """Creates a Pydantic AI agent that parses text into a SpatialNode."""
    return Agent(
        llm,
        # SpatialNode is a RootModel union which Pydantic AI's structured output modes don't support
        # directly. The system prompt already describes the JSON schema so plain text is parsed.
        output_type=TextOutput(_parse_spatial_node),
        system_prompt=SYSTEM_PROMPT,
        model_settings=model_settings or DEFAULT_MODEL_SETTINGS,
    )


def _as_agent(llm: "LLM | Agent[None, SpatialNode]") -> Agent[None, SpatialNode]:
    return llm if isinstance(llm, Agent) else create_spatial_node_agent(llm)


def parse_spatial_node_from_text(llm: "LLM | Agent[None, SpatialNode]", text: str) -> SpatialNode:
    """Parses out the spatial node from text.

    Args:
        llm: A Pydantic AI model, model name, or an agent created with create_spatial_node_agent.
        text: The natural language description of the spatial area.
    """
    return _as_agent(llm).run_sync(text).output


def extract_geometry_from_text(
    llm: "LLM | Agent[None, SpatialNode]", text: str, place_lookup: PlaceLookup | None = None
) -> BaseGeometry:
    """Extracts a spatial area referenced in text as geometry."""
    place_lookup = place_lookup or NominatimAPI()
    spatial_node = parse_spatial_node_from_text(llm, text)
    logger.info("Found spatial node from text %s: %s", text, spatial_node.model_dump_json(indent=2))
    return spatial_node.to_geometry(place_lookup)


#########################
# Code for manual testing
# ruff: noqa: ERA001

# extract_geometry_from_text(DEFAULT_MODEL, "Within 10 km of Baltimore")
