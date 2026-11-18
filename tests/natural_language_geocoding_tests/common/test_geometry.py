from shapely.geometry import MultiPolygon, box

from natural_language_geocoding.common.geometry import count_coordinates, simplify_geometry


def test_simplify_geometry_keeps_multipolygon_of_only_tiny_polygons() -> None:
    tiny_polygons = [box(i * 0.01, 0, i * 0.01 + 0.0001, 0.0001) for i in range(200)]
    geom = MultiPolygon(tiny_polygons)

    simplified = simplify_geometry(geom, max_points=500)

    assert not simplified.is_empty
    assert count_coordinates(simplified) <= 500
