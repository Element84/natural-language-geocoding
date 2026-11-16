"""Helpers for dealing with geometry."""

import json
import math
from functools import cached_property
from typing import Any, cast

import shapely
import shapely.geometry
from pydantic import BaseModel, ConfigDict, Field
from shapely import (
    GeometryCollection,
    LinearRing,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
    count_coordinates,
)
from shapely.geometry.base import BaseGeometry, BaseMultipartGeometry

from natural_language_geocoding.common.util import timed_function


def geometry_from_wkt(wkt: str) -> BaseGeometry:
    """Create shapely geometry from Well-Known Text (WKT) string."""
    return shapely.from_wkt(wkt)  # type: ignore[reportUnknownVariableType]


def geometry_from_geojson_dict(geom: dict[str, Any]) -> BaseGeometry:
    """Create shapely geometry from GeoJSON dict.

    Example:
        Sample usage of the function:

        >>> geometry_dict = {
        ...    "type": "Point",
        ...    "coordinates": [100.0, 0.0]
        ... }
        >>> shapely_geometry = geometry_from_geojson_dict(geometry_dict)
        >>> print(shapely_geometry)
        POINT (100 0)
    """
    return shapely.geometry.shape(geom)


def geometry_from_geojson(geojson: str) -> BaseGeometry:
    """Create shapely geometry from GeoJSON string."""
    return geometry_from_geojson_dict(json.loads(geojson))


def geometry_to_geojson(geom: BaseGeometry) -> str:
    """Convert shapely geometry to GeoJSON string.

    Example:
        Sample usage of the function:

        >>> from shapely.geometry import Point
        >>> point = Point(0, 0)
        >>> geojson_string = geometry_to_geojson(point)
        >>> print(geojson_string)
        '{"type": "Point", "coordinates": [0.0, 0.0]}'
    """
    return json.dumps(geom.__geo_interface__)


def combine_geometry(geoms: list[BaseGeometry]) -> BaseGeometry:
    """Combines geometry into a single collection type."""
    if len(geoms) == 0:
        raise ValueError("Must pass in at least one geometry")
    if len(geoms) == 1:
        return geoms[0]
    types = {type(g) for g in geoms}

    if types == {Point}:
        return MultiPoint(cast("list[Point]", geoms))
    if types == {LineString}:
        return MultiLineString(cast("list[LineString]", geoms))
    if types == {Polygon}:
        return MultiPolygon(cast("list[Polygon]", geoms))
    return GeometryCollection(geoms)


class _GeomPart(BaseModel):
    """Represents a geometry object that's part of a larger geometry collection or polygon."""

    model_config = ConfigDict(
        strict=True, frozen=True, extra="forbid", arbitrary_types_allowed=True
    )

    path: list[int] = Field(
        description="a list of indexes into the parent geometry where this resides"
    )
    geom: BaseGeometry
    is_polygon_part: bool = Field(
        default=False, description="Inidicates if this geometry is a part of a polygon."
    )

    @cached_property
    def area(self) -> float:
        """A simplified square degree area of the bounding box of the geometry."""
        minx, miny, maxx, maxy = self.geom.bounds
        return (maxx - minx) * (maxy - miny)

    @cached_property
    def num_points(self) -> int:
        return count_coordinates(self.geom)

    @staticmethod
    def from_geometry(geom: BaseGeometry, path: list[int] | None = None) -> "list[_GeomPart]":
        """Recursively converts a geometry into it's list of child geometry parts."""
        if path is None:
            path = []
        if isinstance(geom, (Point, LinearRing, LineString)):
            return [_GeomPart(path=path, geom=geom)]
        if isinstance(geom, BaseMultipartGeometry):
            geom_multi: BaseMultipartGeometry[BaseGeometry] = cast(
                "BaseMultipartGeometry[BaseGeometry]", geom
            )
            return [
                metric
                for idx, g in enumerate(geom_multi.geoms)
                for metric in _GeomPart.from_geometry(g, [*path, idx])
            ]
        if isinstance(geom, Polygon):
            return [
                _GeomPart(path=[*path, 0], geom=geom.exterior, is_polygon_part=True),
                *[
                    _GeomPart(path=[*path, idx + 1], geom=g, is_polygon_part=True)
                    for idx, g in enumerate(geom.interiors)
                ],
            ]
        raise TypeError(f"Unsupported geometry type: {type(geom).__name__}")


@timed_function
def remove_extraneous_geoms(geom: BaseGeometry, *, max_points: int) -> BaseGeometry:  # noqa: C901
    """Provides an alternative for simplification that removes the smallest sub geometries.

    For geometries that are collections of other geometries like MultiPolygons or GeometryCollection
    or Polygons with holes this provides a way to simplify geometries by removing the child
    component geometries that are the smallest by area. Normal simplification works by removing
    redundant points but that can only go so far, for example, when trying to simplify a
    GeometryCollection which contains hundreds of polygons with multiple holes.
    """
    if count_coordinates(geom) <= max_points:
        # No reduction is needed
        return geom
    # Converts the geometry into a list of geometry parts sorted by area largest to smallest.
    parts = sorted(_GeomPart.from_geometry(geom), key=lambda r: r.area, reverse=True)

    # The two sets of geometries being collected to return that are under the count of max_points.
    # - A map of parent polygon paths to their rings.
    poly_rings: dict[tuple[int, ...], tuple[LinearRing, list[LinearRing]]] = {}
    # - Any geometry that's not a polygon
    other_geoms: list[BaseGeometry] = []
    # - The number of points in poly_rings and other_geoms
    num_points = 0

    for metric in parts:
        if num_points + metric.num_points > max_points:
            # This area will exceed the number of points. Breaking to return the resulting geometry
            break
        if metric.is_polygon_part:
            parent_poly_path = tuple(metric.path[0:-1])
            ring_index = metric.path[-1]
            if not isinstance(metric.geom, LinearRing):
                raise Exception("Logic error: polygon part is not a ring")

            if parent_poly_path not in poly_rings:
                if ring_index != 0:
                    raise Exception("Always expecting larger geom to be the exterior first")
                poly_rings[parent_poly_path] = (metric.geom, [])
            else:
                if ring_index == 0:
                    raise Exception("Always expecting subsequent geom to be an interior")
                poly_rings[parent_poly_path][1].append(metric.geom)
        else:
            other_geoms.append(metric.geom)
        num_points += metric.num_points

    if len(other_geoms) == 0 and len(poly_rings) == 0:
        raise ValueError(
            f"Could not remove enough geometry parts to get below max points [{max_points}]"
        )

    # Compose the parts from other_geoms and poly_rings into a single geometry.
    geoms_to_combine = other_geoms

    # Construct the most specific type
    if len(poly_rings) > 0:
        mp = MultiPolygon(
            [Polygon(exterior, interiors) for exterior, interiors in poly_rings.values()]
        )
        geoms_to_combine.append(mp)
    return combine_geometry(geoms_to_combine)


# FUTURE the performance of this could be spead up for very large geometries with many small
# polygons by comparing the number of sub geometries to the number of total points. If a ratio
# exceeds a certain percentage then it may make sense to remove geometries initially that
# are less than a certain percent of the total area. Then simplify after that. That could help
# with areas with a larger area and many small islands.


@timed_function
def simplify_geometry(geom: BaseGeometry, max_points: int = 3_000) -> BaseGeometry:
    """Simplify geometry.

    Simplifies a shapely geometry object by reducing the number of points in
    the geometry while preserving its overall shape.

    Args:
        geom (BaseGeometry): The shapely geometry object to be simplified.
        max_points (int): The maximum number of points allowed in the
            simplified geometry.

    Raises:
        ValueError: If geometry cannot be simplified to under max_points
            points.
    """
    num_points = count_coordinates(geom)
    if num_points < max_points:
        return geom

    # Performance Optimization for multipolygons with hundreds (or thousands of subpolygons)
    if isinstance(geom, MultiPolygon) and len(geom.geoms) > 100:  # noqa: PLR2004
        # Drop the smallest polygons
        one_acre = 0.000001
        geom = MultiPolygon([g for g in geom.geoms if g.area > one_acre])

    # Repeatedly try different tolerances to simplify it just until it reaches
    # below the maximum number of points.
    simplified = geom
    for power in range(-5, 0):
        tolerance: float = pow(10, power)
        simplified = simplified.simplify(tolerance)
        if count_coordinates(simplified) < max_points:
            return simplified
    # If it made it this far then normal simplification won't work. Switch to dropping sub
    # geometries to further simplify it.
    return remove_extraneous_geoms(simplified, max_points=max_points)


def BoundingBox(  # noqa: N802
    *, west: float, south: float, east: float, north: float
) -> BaseGeometry:
    """Construct a bounding box geometry using the provided coordinates.

    Args:
        west (float): The western longitude of the bounding box.
        south (float): The southern latitude of the bounding box.
        east (float): The eastern longitude of the bounding box.
        north (float): The northern latitude of the bounding box.

    Returns:
        BaseGeometry: A shapely geometry object representing the bounding box.

    Example:
        Sample usage of the function:

        >>> box = BoundingBox(west=-74.1, south=40.5, east=-73.9, north=40.8)
        >>> print(box)
        POLYGON ((-74.1 40.5, -73.9 40.5, -73.9 40.8, -74.1 40.8, -74.1 40.5))
    """
    return shapely.geometry.box(west, south, east, north)


def between(g1: BaseGeometry, g2: BaseGeometry) -> BaseGeometry:
    """Return the geometry between the two geometries."""
    coll = GeometryCollection([g1, g2])
    return coll.convex_hull - g1.convex_hull - g2.convex_hull


def degrees_to_radians(deg: float) -> float:
    """Convert degrees to radians."""
    return deg * (math.pi / 180.0)


def add_buffer(g: BaseGeometry, distance_km: float) -> BaseGeometry:
    """Add a buffer around the input geometry.

    This function calculates the latitude and longitude distances based on the
    input distance in kilometers. The longitude distance is adjusted based on
    the average latitude of the input geometry. A buffer of the calculated
    average distance between longitude and latitude is added around the input
    geometry.

    Args:
        g (BaseGeometry): The input geometry for which the buffer is to be
            added.
        distance_km (float): The distance in kilometers for the buffer.

    Returns:
        BaseGeometry: A new geometry object with the buffer added around the
            input geometry.

    Note:
        - This function assumes a basic circle buffer and may not work
        correctly near the poles.
        - For accurate distance calculations, consider using a more
        sophisticated approach.

    Example:
        Sample usage of the function:

        >>> from shapely.geometry import Point
        >>> point = Point(0, 0)
        >>> buffered_point = add_buffer(point, 5.0)
        >>> print(buffered_point)
    """
    lat_distance = distance_km / 111.32

    # longitude distance varies with latitude
    avg_lat = g.centroid.y
    avg_lat_rad = degrees_to_radians(avg_lat)
    lon_distance = distance_km / (math.cos(avg_lat_rad) * 111.32)

    # Since we're creating a basic circle around the geometry we'll do
    # something dumb here and just average the longitude and latitude distance.
    # This will fall apart at the poles but works for our current use cases.

    return g.buffer((lon_distance + lat_distance) / 2.0)
