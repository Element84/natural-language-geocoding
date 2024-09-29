from e84_geoai_common.llm import LLM, extract_data_from_text
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.models import SpatialNode
from natural_language_geocoding.prompt import SYSTEM_PROMPT


def extract_geometry_from_text(llm: LLM, text: str) -> BaseGeometry:
    """Given a text string containing a spatial area extracts a spatial area referenced from the geometry."""
    spatial_node = extract_data_from_text(
        llm=llm, model_type=SpatialNode, system_prompt=SYSTEM_PROMPT, user_prompt=text
    )
    if g := spatial_node.to_geometry():
        return g
    else:
        raise Exception(f"Unable to extract geometry from text: {text}")


# from e84_geoai_common.llm.core import BedrockClaudeLLM
# llm = BedrockClaudeLLM()
# extract_geometry_from_text(llm, "Within 10 km of Baltimore")
