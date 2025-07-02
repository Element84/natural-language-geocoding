from shapely import Point

from natural_language_geocoding.geocode_index.geoplace import (
    GeoPlace,
    GeoPlaceSource,
    GeoPlaceSourceType,
    GeoPlaceType,
    Hierarchy,
)

place = GeoPlace(
    id="the id",
    place_name="the name",
    type=GeoPlaceType.borough,
    geom=Point(10, 12),
    source=GeoPlaceSource(source_type=GeoPlaceSourceType.ne, source_path="the source path"),
    alternate_names=["alt name 1", "alt name 2"],
    hierarchies=[
        Hierarchy(continent_id="north america", country_id="usa"),
        Hierarchy(continent_id="North america", country_id="canada"),
    ],
    area_sq_km=23.4,
    population=245,
    properties={"foo": [1, 2, 3]},
)


def test_geoplace_serialization() -> None:
    assert GeoPlace.model_validate(place.model_dump()) == place
    assert GeoPlace.model_validate_json(place.model_dump_json()) == place


def test_self_as_hierarchies() -> None:
    # Default behavior
    assert place.self_as_hierarchies() == [
        Hierarchy(continent_id="north america", country_id="usa", borough_id="the id"),
        Hierarchy(continent_id="North america", country_id="canada", borough_id="the id"),
    ]

    # No hierarchies
    simple_place = GeoPlace(
        id="the id",
        place_name="the name",
        type=GeoPlaceType.borough,
        geom=Point(10, 12),
        source=GeoPlaceSource(source_type=GeoPlaceSourceType.ne, source_path="the source path"),
        properties={},
    )
    assert simple_place.self_as_hierarchies() == [Hierarchy(borough_id="the id")]
