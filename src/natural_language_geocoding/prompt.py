import json
from pathlib import Path
from textwrap import dedent

from e84_geoai_common.llm.extraction import ExtractDataExample
from e84_geoai_common.util import singleline

from natural_language_geocoding.geocode_index.geoplace import GeoPlaceType
from natural_language_geocoding.models import (
    DirectionalConstraint,
    Intersection,
    NamedPlace,
    SpatialNode,
)

# FUTURE Some of these may be unnecessary. Try pruning guidelines and seeing the effect once there
# are more evaluations

GUIDELINES_GENERAL = [
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
        containment—ensuring place names are contextualized within broader regions or countries when
        implied.
        """
    ),
    singleline(
        """
        For instance, when a query mentions specific landmarks or features along with a broader
        geographical area (e.g., "within the United States"), the structure should encapsulate
        the named places within the broader geographical context. This approach ensures the
        query's spatial intent is fully captured, particularly for complex requests involving
        multiple spatial relationships and geographical contexts.
        """
    ),
]

GUIDELINE_HIERARCHY = dedent(
    """
    GEOGRAPHICAL HIERARCHY
        - Always use separate fields (in_continent, in_country, in_region) instead of combining
            them in the name field
        - The name field should contain only the specific place name (e.g., "Paris" not
            "Paris France")
        - Use in_country for country context (e.g., "France" for Paris)
        - Use in_region for state/province context (e.g., "Maryland" for Annapolis)
        - Use in_continent for continental context where appropriate
        - Always populate in_continent when the location is on a continent, even if the user
            doesn't explicitly mention it.
        - Note that in_continent, in_country, in_region should not be used for large bodies of
            water like seas.
    """
).strip()

GUIDELINE_PLACETYPE = (
    "Always specify the place type when it can be determined. "
    "The place type must be one of the following values: "
    + (", ".join([pt.value for pt in GeoPlaceType]))
)

GUIDELINE_PLACETYPE_MACROAREA = singleline(
    """
    MACRO-GEOGRAPHICAL AREAS
        - Use "geoarea" for any geographical area that represents a collection of countries or spans
          multiple countries but is not itself a continent (e.g., "Southern Africa",
          "Southeast Asia", "Scandinavia", "Middle East", "Caribbean", "Balkans", "Central America",
          "South Asia")
        - "macroregion" should only be used for large regions within a single country (e.g.,
          "Normandy" in France)
        - For geoareas, do not populate in_continent, in_country, or in_region fields as they
          represent collections of countries themselves
        - When in doubt about whether a region spans multiple countries, default to using "geoarea"
          rather than "macroregion"
    """
)

GUIDELINES_SIMPLIFY = [
    # FUTURE these two seem redundant and could probably be combined.
    singleline(
        """
        Simplify When Possible: Always generate the simplest version of the tree possible to
        accurately represent the user's request. This often means direct mapping of queries to a
        "NamedPlace" for singular geographical locations without implied spatial operations.
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
]

GUIDELINES_APPROPRIATE_NODE_TYPE = [
    singleline(
        """
        Appropriate Use of Node Types: Only employ complex node types (e.g., "Intersection",
        "Buffer") when the user's query explicitly or implicitly requires the representation of
        spatial relationships or operations between two or more places.
        """
    ),
    singleline(
        """
        When interpreting combinations of areas through the use of words like "and", use clues from
        the phrasing to determine if the area is an intersection or a union. It is most likely the
        user wants a union unless they very specifically say something like "the overlap of A and B"
        """
    ),
]

GUIDELINE_HIERARCHICAL_CONTEXT = singleline(
    """
    Incorporate Hierarchical Geographical Contexts: Always consider and explicitly include
    broader geographical contexts if implied or directly mentioned in the query. This ensures
    the spatial query is accurately scoped within the correct geographical boundaries.
    """
)

GUIDELINE_PLACE_NAME_SIMPLIFICATION = """
PLACE NAME SIMPLIFICATION
    - When users refer to features or aspects of a place (e.g., "Shanghai's shipping lanes", "the
      business district of New York"), extract just the core place name ("Shanghai", "New York")
    - Do not include descriptive qualifiers in the name field that wouldn't exist in a standard
      geocoding database
    - Use the appropriate place type to indicate the nature of the location (e.g., "port" for
      Shanghai when shipping is mentioned)
    - Focus on mapping to standard geographical entities that would exist in geocoding databases
    - Remember that descriptive phrases like "business district," "shipping lanes," or "territorial
      waters" should inform your choice of place type but should not be included in the name field
"""

GUIDELINES: list[str] = [
    *GUIDELINES_GENERAL,
    GUIDELINE_HIERARCHY,
    GUIDELINE_PLACETYPE,
    GUIDELINE_PLACETYPE_MACROAREA,
    *GUIDELINES_SIMPLIFY,
    *GUIDELINES_APPROPRIATE_NODE_TYPE,
    GUIDELINE_HIERARCHICAL_CONTEXT,
    GUIDELINE_PLACE_NAME_SIMPLIFICATION,
]


EXAMPLES = [
    ExtractDataExample(
        name="Simple Spatial Example",
        user_query="in North Dakota",
        structure=NamedPlace(
            name="North Dakota",
            type=GeoPlaceType.region,
            in_continent="North America",
            in_country="United States",
        ),
    ),
    ExtractDataExample(
        name="Complex Query Example",
        user_query="in New Mexico, west of Albuquerque",
        structure=Intersection(
            child_nodes=[
                NamedPlace(
                    name="New Mexico",
                    type=GeoPlaceType.region,
                    in_continent="North America",
                    in_country="United States",
                ),
                DirectionalConstraint(
                    direction="west",
                    child_node=NamedPlace(
                        name="Albuquerque",
                        type=GeoPlaceType.locality,
                        in_continent="North America",
                        in_country="United States",
                        in_region="New Mexico",
                    ),
                ),
            ]
        ),
    ),
]

with (Path(__file__).parent / "prompt.md").open() as f:
    prompt_template = f.read()

SYSTEM_PROMPT = prompt_template.format(
    json_schema=json.dumps(SpatialNode.model_json_schema()),
    guidelines="\n\n".join(GUIDELINES),
    examples="\n\n".join([example.to_str() for example in EXAMPLES]),
)
