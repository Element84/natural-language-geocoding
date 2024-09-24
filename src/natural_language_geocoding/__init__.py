import json
import os

from e84_geoai_common.llm import LLM, ExtractDataExample, extract_data_from_text
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.models import SpatialNode

EXAMPLES = [
    ExtractDataExample(
        name="Simple Spatial Example",
        user_query="in North Dakota",
        structure=SpatialNode.model_validate(
            {"node_type": "NamedEntity", "name": "North Dakota"}
        ),
    ),
    ExtractDataExample(
        name="Complex Query Example",
        user_query="in northern New Mexico, west of Albuquerque",
        structure=SpatialNode.model_validate(
            {
                "node_type": "Intersection",
                "child_node_1": {
                    "node_type": "NamedEntity",
                    "name": "New Mexico",
                    "subportion": "western half",
                },
                "child_node_2": {
                    "node_type": "DirectionalConstraint",
                    "child_node": {
                        "node_type": "NamedEntity",
                        "name": "Albuquerque",
                    },
                    "direction": "west",
                },
            }
        ),
    ),
]


with open(os.path.join(os.path.dirname(__file__), "prompt.md")) as f:
    prompt_template = f.read()

SYSTEM_PROMPT = prompt_template.format(
    json_schema=json.dumps(SpatialNode.model_json_schema()),
    examples="\n\n".join([example.to_str() for example in EXAMPLES]),
)


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
