from functools import singledispatch

from shapely import LinearRing, MultiPolygon, Point, Polygon
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.geocode_index.ingesters.ingest_utils import (
    remove_duplicate_points,
)

point = Point(5, 7)
ring = LinearRing([(1, 2), (7, 2), (7, 8), (1, 8), (1, 2)])
poly_no_holes = Polygon(ring)
poly_w_holes = Polygon(
    ring,
    [
        LinearRing([(3, 3), (3, 4), (4, 4), (4, 3), (3, 3)]),
        LinearRing([(3, 5), (3, 7), (5, 7), (5, 5), (3, 5)]),
    ],
)
multipolygon_simple = MultiPolygon([poly_no_holes])
multipolygon_complex = MultiPolygon(
    [
        poly_w_holes,
        Polygon(LinearRing([(9, 3), (12, 3), (12, 5), (9, 5), (9, 3)])),
        Polygon(LinearRing([(10, 7), (11, 7), (11, 8), (10, 8), (10, 7)])),
    ]
)

geoms_without_dups = [
    point,
    ring,
    poly_no_holes,
    poly_w_holes,
    multipolygon_simple,
    multipolygon_complex,
]


@singledispatch
def _add_duplicates[T_Geom: BaseGeometry](geom: T_Geom, separation: float) -> T_Geom:
    raise NotImplementedError


@_add_duplicates.register
def _(point: Point, _separation: float) -> Point:
    return point


@_add_duplicates.register
def _(ring: LinearRing, separation: float) -> LinearRing:
    coords_except_last = ring.coords[0:-1]
    new_coords = [
        new_coord
        for coord in coords_except_last
        for new_coord in [coord, (coord[0] + separation, coord[1] + separation)]
    ]
    return LinearRing([*new_coords, ring.coords[-1]])


@_add_duplicates.register
def _(poly: Polygon, separation: float) -> Polygon:
    return Polygon(
        _add_duplicates(poly.exterior, separation),
        [_add_duplicates(hole, separation) for hole in poly.interiors],
    )


@_add_duplicates.register
def _(mp: MultiPolygon, separation: float) -> MultiPolygon:
    return MultiPolygon(
        [_add_duplicates(poly, separation) for poly in mp.geoms],
    )


def _assert_deduplicates(
    dups_geom: BaseGeometry, expected_geom: BaseGeometry, tolerance: float = 0.0
) -> None:
    assert expected_geom.is_valid, "Expected area is not valid"
    no_dups = remove_duplicate_points(dups_geom, tolerance)
    assert no_dups.is_valid, "Deduplicated area is not valid"
    assert no_dups == expected_geom


def test_remove_duplicate_points_with_no_dups():
    for geom in geoms_without_dups:
        assert geom.is_valid, "Test geometry is not valid"
        no_dups = remove_duplicate_points(geom, 0)
        assert no_dups.is_valid
        assert no_dups == geom
        no_dups_2 = remove_duplicate_points(geom, 0.25)
        assert no_dups_2.is_valid
        assert no_dups_2 == geom


def test_remove_explicit_dups_ring():
    ring_w_dups = LinearRing([(1, 2), (7, 2), (7, 2), (7, 8), (1, 8), (1, 2)])
    ring_wo_dups = LinearRing([(1, 2), (7, 2), (7, 8), (1, 8), (1, 2)])

    _assert_deduplicates(ring_w_dups, ring_wo_dups)
    _assert_deduplicates(ring_w_dups, ring_wo_dups, 0.25)


def test_remove_close_dups_ring():
    ring_w_dups = LinearRing([(1, 2), (7, 2), (7.1, 2), (7, 8), (1, 8), (1, 8.1), (1, 2)])
    ring_wo_dups = LinearRing([(1, 2), (7, 2), (7, 8), (1, 8), (1, 2)])

    # A low tolerance does not deduplicate
    not_deduplicated = remove_duplicate_points(ring_w_dups, 0)
    assert not_deduplicated == ring_w_dups

    # A higher tolerance does deduplicate
    _assert_deduplicates(ring_w_dups, ring_wo_dups, 0.25)


def test_remove_explicit_dups_all_types():
    for geom in geoms_without_dups:
        duped_geom = _add_duplicates(geom, 0)
        _assert_deduplicates(duped_geom, geom, 0)


def test_remove_dups_all_types():
    for geom in geoms_without_dups:
        duped_geom = _add_duplicates(geom, 0.1)

        # A low tolerance does not deduplicate
        not_deduplicated = remove_duplicate_points(duped_geom, 0.12)
        assert not_deduplicated == duped_geom

        # This tolerance has to be slightly larger since 0.1 is added on both axes so the hypotenuse
        # is of length 0.14142...
        _assert_deduplicates(duped_geom, geom, 0.15)


def test_remove_dups_polygon_with_tiny_holes():
    hole1 = LinearRing([(3, 3), (3, 4), (4, 4), (4, 3), (3, 3)])
    hole2 = LinearRing([(3, 5), (3, 7), (5, 7), (5, 5), (3, 5)])
    tiny_hole = LinearRing([(5, 3), (5, 3.1), (5.1, 3.1), (5.1, 3), (5, 3)])
    poly_w_tiny_holes = Polygon(ring, [hole1, tiny_hole, hole2])
    expected = Polygon(ring, [hole1, hole2])
    _assert_deduplicates(poly_w_tiny_holes, expected, 0.15)


def test_remove_dups_multipolygon_with_tiny_holes():
    hole1 = LinearRing([(3, 3), (3, 4), (4, 4), (4, 3), (3, 3)])
    hole2 = LinearRing([(3, 5), (3, 7), (5, 7), (5, 5), (3, 5)])
    tiny_hole = LinearRing([(5, 3), (5, 3.1), (5.1, 3.1), (5.1, 3), (5, 3)])
    poly_w_tiny_holes = Polygon(ring, [hole1, tiny_hole, hole2])
    expected_dedup_poly = Polygon(ring, [hole1, hole2])

    tiny_poly = Polygon(LinearRing([(15, 3), (15, 3.1), (15.1, 3.1), (15.1, 3), (15, 3)]))

    mp = MultiPolygon([poly_w_tiny_holes, tiny_poly])
    expected_mp = MultiPolygon([expected_dedup_poly])

    _assert_deduplicates(mp, expected_mp, 0.15)
