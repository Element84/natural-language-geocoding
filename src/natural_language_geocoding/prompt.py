import json
import os
from e84_geoai_common.util import singleline
from e84_geoai_common.llm import ExtractDataExample

from natural_language_geocoding.models import SpatialNode

GUIDELINES = [
    singleline(
        """
        These requests will define spatial areas through direct mentions or implied geographical
        contexts. Your structure should articulate the spatial operations needed, integrating named
        geographical entities and their spatial relationships, including hierarchical contexts.
        """
    ),
    singleline(
        """
        The structured response must adhere to the provided JSON schema, emphasizing the importance
        of accurately representing spatial relationships. These include direct spatial operations
        like "between," "buffer," and "intersection," as well as hierarchical geographical
        containment—ensuring entities are contextualized within broader regions or countries when
        implied.
        """
    ),
    singleline(
        """
        For instance, when a query mentions specific landmarks or features along with a broader
        geographical area (e.g., "within the United States"), the structure should encapsulate
        the named entities within the broader geographical context. This approach ensures the
        query's spatial intent is fully captured, particularly for complex requests involving
        multiple spatial relationships and geographical contexts.
        """
    ),
    singleline(
        """
        Specifically, when translating city names into named entities, always include the most
        specific geographical context available, such as 'Boston Massachusetts' instead of just
        'Boston'. This ensures that the NamedEntity reflects both the city and state, or city and
        country, maintaining clear and unambiguous geographical identification.
        """
    ),
    singleline(
        """
        Simplify When Possible: Always generate the simplest version of the tree possible to
        accurately represent the user's request. This often means direct mapping of queries to a
        "NamedEntity" for singular geographical locations without implied spatial operations.
        """
    ),
    singleline(
        """
        Appropriate Use of Node Types: Only employ complex node types (e.g., "Intersection",
        "Buffer") when the user's query explicitly or implicitly requires the representation of
        spatial relationships or operations between two or more entities.
        """
    ),
    singleline(
        """
        Validation and Simplification: After generating the tree structure, review it to ensure it
        represents the simplest accurate form of the user's query. Unnecessary complexity or
        unrelated entities should be avoided. Though, make sure to keep any thing that's necessary
        to accurately represent the user's search area.
        """
    ),
    singleline(
        """
        Incorporate Hierarchical Geographical Contexts: Always consider and explicitly include
        broader geographical contexts if implied or directly mentioned in the query. This ensures
        the spatial query is accurately scoped within the correct geographical boundaries.
        """
    ),
]


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
    guidelines="\n\n".join(GUIDELINES),
    examples="\n\n".join([example.to_str() for example in EXAMPLES]),
)
