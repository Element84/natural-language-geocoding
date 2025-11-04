from typing import Literal, cast

from e84_geoai_common.geometry import BoundingBox
from e84_geoai_common.tracing import timed_function
from shapely import GeometryCollection, MultiPolygon, Point
from shapely.geometry.base import BaseGeometry, BaseMultipartGeometry


@timed_function
def take_compass_subset(
    direction: Literal["west", "east", "north", "south"],
    geom: BaseGeometry,
) -> BaseGeometry:
    """Extract a directional subset (half) of a geometry based on compass direction.

    This function takes a geometry and returns the portion that lies in the specified
    compass direction (west, east, north, or south) relative to the geometry's centroid.
    The split is made at the centroid coordinates, creating directional halves.

    For multi-part geometries (MultiPolygon, GeometryCollection), the function attempts
    to find a dominant geometry (>50% of total area) to use for centroid calculation.
    If no single geometry dominates, it uses the centroid of the entire collection.

    Args:
        direction: The compass direction to extract. One of "west", "east", "north", "south".
        geom: The input geometry to subset. Can be any Shapely geometry type.

    Returns:
        BaseGeometry: The portion of the input geometry that lies in the specified
        compass direction. For Point geometries, returns the original point unchanged.

    Examples:
        >>> polygon = Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])
        >>> western_half = take_compass_subset("west", polygon)
        >>> # Returns the western half of the polygon (left of centroid x-coordinate)
    """
    if isinstance(geom, Point):
        return geom

    # Find the specific geometry to use as the center from which to consider the middle
    centroid_geom: BaseGeometry | None = None

    if isinstance(geom, (MultiPolygon, GeometryCollection)):
        geom_multi: BaseMultipartGeometry[BaseGeometry] = cast(
            "BaseMultipartGeometry[BaseGeometry]", geom
        )
        # Exclude any geometry that are less than 10% of the total area
        total_area = geom.area
        for g in geom_multi.geoms:
            if g.area > 0.50 * total_area:
                # Use an area that's more than 50% of the total area as the geom
                centroid_geom = g
                break
        if centroid_geom is None:
            centroid_geom = geom_multi
    else:
        centroid_geom = geom

    west, south, east, north = geom.bounds
    centroid = centroid_geom.centroid

    match direction:
        case "west":  # western half
            mask = BoundingBox(
                west=west,
                east=centroid.x,
                north=north,
                south=south,
            )
        case "east":  # eastern half
            mask = BoundingBox(
                west=centroid.x,
                east=east,
                north=north,
                south=south,
            )
        case "north":  # northern half
            mask = BoundingBox(
                west=west,
                east=east,
                north=north,
                south=centroid.y,
            )
        case "south":  # southern half
            mask = BoundingBox(
                west=west,
                east=east,
                north=centroid.y,
                south=south,
            )
    return geom.intersection(mask)
