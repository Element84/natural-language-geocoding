from e84_geoai_common.llm.core import LLM
from e84_geoai_common.llm.models.claude import BedrockClaudeLLM
from e84_geoai_common.llm.tests.mock_bedrock_runtime import (
    claude_response_with_content,
    make_test_bedrock_runtime_client,
)
from shapely import Polygon

from natural_language_geocoding import extract_geometry_from_text
from natural_language_geocoding.models import (
    AnySpatialNodeType,
    BorderBetween,
    BorderOf,
    Difference,
    DirectionalConstraint,
    Intersection,
    NamedPlace,
    Union,
)
from natural_language_geocoding.tests.canned_place_lookup import (
    ALPHA,
    BRAVO,
    DELTA,
    FLORIDA,
    LOUISIANA,
    NORTH_DAKOTA,
    SOUTH_DAKOTA,
    CannedPlaceLookup,
)


def make_llm(spatial_node: AnySpatialNodeType) -> LLM:
    claude_resp = spatial_node.model_dump_json()[1:]
    return BedrockClaudeLLM(
        client=make_test_bedrock_runtime_client([claude_response_with_content(claude_resp)])
    )


def test_single_place():
    place_lookup = CannedPlaceLookup()
    llm = make_llm(NamedPlace(name="Florida"))
    geom = extract_geometry_from_text(llm, text="Florida", place_lookup=place_lookup)
    assert geom == FLORIDA


def test_union():
    place_lookup = CannedPlaceLookup()
    llm = make_llm(Union.from_nodes(NamedPlace(name="Florida"), NamedPlace(name="Louisiana")))
    geom = extract_geometry_from_text(llm, text="Florida and Louisiana", place_lookup=place_lookup)
    assert geom == FLORIDA.union(LOUISIANA)


def test_intersection():
    place_lookup = CannedPlaceLookup()
    llm = make_llm(Intersection.from_nodes(NamedPlace(name="delta"), NamedPlace(name="bravo")))
    geom = extract_geometry_from_text(
        llm, text="Where delta and bravo overlap", place_lookup=place_lookup
    )
    assert geom == DELTA.intersection(BRAVO)


def test_difference():
    place_lookup = CannedPlaceLookup()
    llm = make_llm(
        Difference(child_node_1=NamedPlace(name="alpha"), child_node_2=NamedPlace(name="bravo"))
    )
    geom = extract_geometry_from_text(llm, text="alpha except for bravo", place_lookup=place_lookup)
    assert geom == ALPHA - BRAVO


def test_directional_constraint():
    place_lookup = CannedPlaceLookup()
    llm = make_llm(
        Intersection.from_nodes(
            NamedPlace(name="alpha"),
            DirectionalConstraint(child_node=NamedPlace(name="bravo"), direction="north"),
        )
    )
    geom = extract_geometry_from_text(llm, text="alpha north of bravo", place_lookup=place_lookup)
    # fmt: off
    alpha_north_of_bravo = Polygon([
        (1, 11), (15, 11),  # top edge
        (15, 9), (1, 9),    # bottom edge
        (1, 11)             # back to start
    ])
    # fmt: on
    assert geom == alpha_north_of_bravo


def test_border():
    place_lookup = CannedPlaceLookup()
    llm = make_llm(BorderOf(child_node=NamedPlace(name="alpha")))
    geom = extract_geometry_from_text(llm, text="border of alpha", place_lookup=place_lookup)
    assert geom == ALPHA.boundary


def test_border_between():
    place_lookup = CannedPlaceLookup()
    llm = make_llm(
        BorderBetween(
            child_node_1=NamedPlace(name="North Dakota"),
            child_node_2=NamedPlace(name="South Dakota"),
        )
    )
    border = extract_geometry_from_text(
        llm, text="the border between north and south dakota", place_lookup=place_lookup
    )
    assert NORTH_DAKOTA.intersects(border)
    assert SOUTH_DAKOTA.intersects(border)

    nd_west, nd_south, nd_east, _nd_north = NORTH_DAKOTA.bounds
    sd_west, _sd_south, _sd_east, sd_north = SOUTH_DAKOTA.bounds
    border_west, border_south, border_east, border_north = border.bounds

    # Ensure the border covers the east and west extremes
    assert border_west < nd_west
    assert border_west < sd_west
    assert border_east > nd_east
    # The eastern most longitude of the northern border of south dakota
    # Parts of South Dakota go east of this which won't be covered by this border geometry.
    sd_north_border_east = -96.563664
    assert border_east > sd_north_border_east

    # Ensure the border covers the south of North Dakota and the north of South Dakota
    assert border_south < nd_south < border_north
    assert border_south < sd_north < border_north
