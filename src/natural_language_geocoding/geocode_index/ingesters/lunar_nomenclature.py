"""USGS Lunar Nomenclature Ingester for Geocoding Index.

This module provides functionality to download, read, process, and index lunar feature data
from the USGS Planetary Nomenclature shapefile into a lunar geocoding index.

Source: https://asc-planetarynames-data.s3.us-west-2.amazonaws.com/MOON_nomenclature_center_pts.zip
"""

import logging
import math
import zipfile
from pathlib import Path
from typing import Any

import geopandas as gpd
import requests
from shapely import Point
from shapely.geometry import Polygon, box
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.geocode_index.ingesters.ingest_utils import fix_geometry
from natural_language_geocoding.geocode_index.lunar_index import (
    LunarGeocodeIndex,
    LunarPlace,
    LunarPlaceSource,
    LunarPlaceSourceType,
)
from natural_language_geocoding.geocode_index.lunar_place_types import (
    SHAPEFILE_TYPE_TO_LUNAR_PLACE_TYPE,
    LunarPlaceType,
)

logger = logging.getLogger(__name__)

_LOCAL_TEMP_DIR = Path("temp")

_LUNAR_NOMENCLATURE_URL = (
    "https://asc-planetarynames-data.s3.us-west-2.amazonaws.com/MOON_nomenclature_center_pts.zip"
)
_LUNAR_NOMENCLATURE_ZIP = "MOON_nomenclature_center_pts.zip"
_LUNAR_NOMENCLATURE_SHAPEFILE = "MOON_nomenclature_center_pts.shp"

logger = logging.getLogger(__name__)

# Moon radius in km
MOON_RADIUS_KM = 1737.4

# Alternate names for well-known lunar features (Latin name -> English translations)
_ALTERNATE_NAMES: dict[str, list[str]] = {
    "Mare Tranquillitatis": ["Sea of Tranquility", "Sea of Tranquillity"],
    "Mare Serenitatis": ["Sea of Serenity"],
    "Mare Imbrium": ["Sea of Rains", "Sea of Showers"],
    "Mare Nubium": ["Sea of Clouds"],
    "Mare Fecunditatis": ["Sea of Fertility", "Sea of Fecundity"],
    "Mare Crisium": ["Sea of Crises"],
    "Mare Nectaris": ["Sea of Nectar"],
    "Mare Humorum": ["Sea of Moisture"],
    "Mare Vaporum": ["Sea of Vapors", "Sea of Vapours"],
    "Mare Frigoris": ["Sea of Cold"],
    "Mare Cognitum": ["Sea of Knowledge", "Known Sea"],
    "Mare Marginis": ["Sea of the Edge"],
    "Mare Smythii": ["Smyth's Sea"],
    "Mare Australe": ["Southern Sea"],
    "Mare Spumans": ["Foaming Sea"],
    "Mare Undarum": ["Sea of Waves"],
    "Mare Anguis": ["Serpent Sea"],
    "Mare Insularum": ["Sea of Islands"],
    "Mare Orientale": ["Eastern Sea"],
    "Mare Moscoviense": ["Sea of Moscow"],
    "Mare Ingenii": ["Sea of Cleverness"],
    "Mare Humboldtianum": ["Humboldt's Sea"],
    "Oceanus Procellarum": ["Ocean of Storms"],
    "Lacus Mortis": ["Lake of Death"],
    "Lacus Somniorum": ["Lake of Dreams"],
    "Sinus Iridum": ["Bay of Rainbows"],
    "Sinus Medii": ["Central Bay"],
    "Palus Putredinis": ["Marsh of Decay"],
    "Palus Somni": ["Marsh of Sleep"],
    "Montes Apenninus": ["Apennine Mountains", "Lunar Apennines"],
    "Montes Caucasus": ["Caucasus Mountains"],
    "Montes Alpes": ["Alps", "Lunar Alps"],
    "Montes Carpatus": ["Carpathian Mountains"],
    "Montes Jura": ["Jura Mountains"],
    "Vallis Alpes": ["Alpine Valley"],
    "Vallis Schröteri": ["Schroter's Valley"],
    "Rima Hadley": ["Hadley Rille"],
}

# Landing site alternate names
_LANDING_SITE_ALTERNATES: dict[str, list[str]] = {
    "Statio Tranquillitatis": ["Apollo 11 Landing Site", "Tranquility Base"],
    "Statio Intrepidus": ["Apollo 12 Landing Site"],
    "Statio Fra Mauro": ["Apollo 14 Landing Site"],
    "Statio Hadley": ["Apollo 15 Landing Site"],
    "Statio Descartes": ["Apollo 16 Landing Site"],
    "Statio Taurus-Littrow": ["Apollo 17 Landing Site"],
}


def _convert_lon_360_to_180(lon: float) -> float:
    """Convert longitude from 0-360 range to -180 to 180 range."""
    if lon > 180:  # noqa: PLR2004
        return lon - 360
    return lon


def _generate_circle_polygon(
    center_lat: float, center_lon: float, diameter_km: float, num_points: int = 64
) -> Polygon:
    """Generate a circle polygon from center point and diameter.

    Converts km radius to degrees using Moon's radius for the latitude correction.
    """
    radius_km = diameter_km / 2.0
    # Convert radius from km to degrees (approximate)
    radius_deg_lat = radius_km / (MOON_RADIUS_KM * math.pi / 180)
    # Correct for longitude convergence at latitude.
    # Guard against division by zero near the poles (cos(lat) ≈ 0);
    # below this threshold we skip the correction to avoid extreme distortion.
    cos_lat = math.cos(math.radians(center_lat))
    radius_deg_lon = radius_deg_lat / cos_lat if cos_lat > 0.01 else radius_deg_lat  # noqa: PLR2004

    coords: list[tuple[float, float]] = []
    for i in range(num_points):
        angle = 2 * math.pi * i / num_points
        x = center_lon + radius_deg_lon * math.cos(angle)
        y = center_lat + radius_deg_lat * math.sin(angle)
        # Clamp latitude
        y = max(-90.0, min(90.0, y))
        coords.append((x, y))
    coords.append(coords[0])  # Close the ring
    return Polygon(coords)


def _generate_geometry(  # noqa: PLR0913
    place_type: LunarPlaceType,
    center_lat: float,
    center_lon: float,
    diameter_km: float | None,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
) -> BaseGeometry:
    """Generate polygon geometry for a lunar feature.

    Strategy (hybrid - Option C from plan):
    - Craters with diameter: circle from center + diameter/2
    - All others: bounding box rectangle
    """
    if place_type == LunarPlaceType.crater and diameter_km and diameter_km > 0:
        return _generate_circle_polygon(center_lat, center_lon, diameter_km)

    if place_type == LunarPlaceType.satellite_feature and diameter_km and diameter_km > 0:
        return _generate_circle_polygon(center_lat, center_lon, diameter_km)

    # If bounding box is degenerate (a point):
    if min_lat == max_lat and min_lon == max_lon:
        if diameter_km and diameter_km > 0:
            # if we have a diameter, use a circle instead
            return _generate_circle_polygon(center_lat, center_lon, diameter_km)
        return Point(center_lon, center_lat)

    # Default: use bounding box
    return box(min_lon, min_lat, max_lon, max_lat)


def _normalize_type(type_str: str) -> LunarPlaceType:
    """Convert shapefile type string to LunarPlaceType."""
    place_type = SHAPEFILE_TYPE_TO_LUNAR_PLACE_TYPE.get(type_str)
    if place_type is None:
        logger.warning("Unknown lunar feature type: %s, defaulting to crater", type_str)
        return LunarPlaceType.crater
    return place_type


def _get_alternate_names(name: str, place_type: LunarPlaceType) -> list[str]:
    """Get alternate names for a lunar feature."""
    alternates: list[str] = []
    if name in _ALTERNATE_NAMES:
        alternates.extend(_ALTERNATE_NAMES[name])
    if place_type == LunarPlaceType.landing_site and name in _LANDING_SITE_ALTERNATES:
        alternates.extend(_LANDING_SITE_ALTERNATES[name])
    return alternates


def _download_lunar_nomenclature() -> Path:
    """Downloads the USGS lunar nomenclature shapefile if not already downloaded.

    Returns the path to the extracted shapefile (.shp).
    """
    _LOCAL_TEMP_DIR.mkdir(parents=True, exist_ok=True)

    zip_path = _LOCAL_TEMP_DIR / _LUNAR_NOMENCLATURE_ZIP
    shapefile_path = _LOCAL_TEMP_DIR / _LUNAR_NOMENCLATURE_SHAPEFILE

    if shapefile_path.exists():
        return shapefile_path

    if not zip_path.exists():
        logger.info("Downloading %s", _LUNAR_NOMENCLATURE_URL)
        response = requests.get(_LUNAR_NOMENCLATURE_URL, stream=True, timeout=30)
        response.raise_for_status()
        with zip_path.open("wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info("Downloaded to %s", zip_path)

    logger.info("Extracting %s", zip_path)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(_LOCAL_TEMP_DIR)
    logger.info("Extracted shapefile to %s", shapefile_path)

    return shapefile_path


def read_lunar_shapefile(shapefile_path: Path) -> gpd.GeoDataFrame:
    """Read the USGS lunar nomenclature shapefile."""
    if not shapefile_path.exists():
        raise FileNotFoundError(f"Lunar shapefile not found at {shapefile_path}")

    logger.info("Reading lunar shapefile from %s", shapefile_path)
    gdf = gpd.read_file(shapefile_path)  # pyright: ignore[reportUnknownMemberType]
    logger.info("Read %d features from lunar shapefile", len(gdf))
    return gdf


def shapefile_to_lunar_geoplaces(
    gdf: gpd.GeoDataFrame,
    source_path: str = "usgs_lunar_nomenclature",
) -> list[LunarPlace]:
    """Convert a GeoDataFrame from the USGS lunar shapefile to LunarPlace objects."""
    places: list[LunarPlace] = []

    for idx, raw_row in gdf.iterrows():  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        row: dict[str, Any] = dict(raw_row)  # pyright: ignore[reportUnknownArgumentType]
        name: str = str(row["name"])
        clean_name: str = str(row.get("clean_name", name))
        type_str: str = str(row["type"])
        code: str = str(row.get("code", ""))
        diameter: float | None = float(row["diameter"]) if row.get("diameter") else None
        center_lat: float = float(row["center_lat"])
        center_lon: float = _convert_lon_360_to_180(float(row["center_lon"]))
        min_lat: float = (
            float(row["min_lat"]) if row.get("min_lat") is not None else center_lat - 0.5
        )
        max_lat: float = (
            float(row["max_lat"]) if row.get("max_lat") is not None else center_lat + 0.5
        )
        min_lon: float = _convert_lon_360_to_180(
            float(row["min_lon"])
            if row.get("min_lon") is not None
            else float(row["center_lon"]) - 0.5
        )
        max_lon: float = _convert_lon_360_to_180(
            float(row["max_lon"])
            if row.get("max_lon") is not None
            else float(row["center_lon"]) + 0.5
        )
        quad_name: str | None = str(row["quad_name"]) if row.get("quad_name") else None
        origin: str | None = str(row["origin"]) if row.get("origin") else None

        place_type = _normalize_type(type_str)
        diameter_km: float | None = diameter if diameter and diameter > 0 else None

        # Generate geometry
        feature_id = f"lunar-{code}-{idx}"
        geom = _generate_geometry(
            place_type=place_type,
            center_lat=center_lat,
            center_lon=center_lon,
            diameter_km=diameter_km,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon,
        )
        geom = fix_geometry(feature_id, geom)

        alternate_names = _get_alternate_names(name, place_type)

        # Calculate area from geometry
        area_sq_km: float | None = None
        if diameter_km:
            # Approximate area for circular features
            area_sq_km = math.pi * (diameter_km / 2) ** 2

        place = LunarPlace(
            id=feature_id,
            place_name=name,
            type=place_type,
            type_code=code,
            geom=geom,
            source=LunarPlaceSource(
                source_type=LunarPlaceSourceType.usgs_planetary,
                source_path=source_path,
            ),
            alternate_names=alternate_names,
            area_sq_km=area_sq_km,
            diameter_km=diameter_km,
            center_lat=center_lat,
            center_lon=center_lon,
            quad_name=quad_name,
            origin=origin,
            properties={
                "clean_name": clean_name,
                "type_raw": type_str,
            },
        )
        places.append(place)

    logger.info("Converted %d features to LunarPlace objects", len(places))
    return places


def ingest_lunar_places(
    index: LunarGeocodeIndex | None = None,
    *,
    recreate: bool = False,
    batch_size: int = 500,
) -> None:
    """Download and ingest lunar places from USGS into OpenSearch.

    Downloads the USGS lunar nomenclature shapefile (if not already cached in temp/),
    converts features to LunarPlace objects, and indexes them.

    Args:
        index: Optional LunarGeocodeIndex instance. Creates one if not provided.
        recreate: If True, recreate the index before ingesting.
        batch_size: Number of places to index per batch.
    """
    index = index or LunarGeocodeIndex()

    # Download and read shapefile
    shapefile_path = _download_lunar_nomenclature()
    gdf = read_lunar_shapefile(shapefile_path)

    # Convert to LunarPlace objects
    places = shapefile_to_lunar_geoplaces(gdf)

    # Create index
    index.create_index(recreate=recreate)

    # Bulk index in batches
    total = len(places)
    for i in range(0, total, batch_size):
        batch = places[i : i + batch_size]
        logger.info("Indexing batch %d-%d of %d", i, min(i + batch_size, total), total)
        index.bulk_index(batch)

    logger.info("Successfully ingested %d lunar places", total)


if __name__ == "__main__" and "get_ipython" not in globals():
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    ingest_lunar_places(recreate=True)
