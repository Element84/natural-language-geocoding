"""Helpers for visually debugging geometry. Requires folium (included in the demo extra)."""

import folium  # type: ignore[reportMissingTypeStubs]
from shapely import GeometryCollection
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.common.util import timed_function

SEARCH_AREA_STYLE = {
    "fillColor": "transparent",
    "color": "#FF0000",
    "weight": 3,
}


@timed_function
def display_geometry(
    geoms: list[BaseGeometry],
    *,
    selected_geometry: BaseGeometry | None = None,
    search_area: BaseGeometry | None = None,
) -> folium.Map:
    """Display the provided geometries on a folium Map.

    The map is fitted to the selected_geometry if given, otherwise the search_area if given,
    otherwise the bounds of all geometries. The search_area, if given, is drawn as an outline.
    """
    if selected_geometry:
        min_lon, min_lat, max_lon, max_lat = selected_geometry.bounds
        point = selected_geometry.centroid
    elif search_area:
        min_lon, min_lat, max_lon, max_lat = search_area.bounds
        point = search_area.centroid
    else:
        hull = GeometryCollection(geoms).convex_hull
        point = hull.centroid
        min_lon, min_lat, max_lon, max_lat = hull.bounds

    m = folium.Map(location=[point.y, point.x])
    m.fit_bounds([[min_lat, min_lon], [max_lat, max_lon]])

    for geom in geoms:
        folium.GeoJson(geom.__geo_interface__).add_to(m)

    if search_area:
        folium.GeoJson(
            search_area.__geo_interface__,
            style_function=lambda _x: SEARCH_AREA_STYLE,  # type: ignore[reportUnknownLambdaType]
        ).add_to(m)

    return m
