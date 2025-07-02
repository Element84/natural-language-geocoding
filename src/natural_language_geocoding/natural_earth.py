"""Helpers for dealing with GeoJSON from Natural Earth."""

import json
import logging
import urllib.request
from functools import lru_cache
from pathlib import Path

from e84_geoai_common.geojson import FeatureCollection
from e84_geoai_common.geometry import add_buffer
from e84_geoai_common.util import timed_function
from pydantic import AliasChoices, BaseModel, ConfigDict, Field
from shapely import GeometryCollection
from shapely.geometry.base import BaseGeometry

NATURAL_EARTH_DATA_DIR = Path(__file__).parent / "natural_earth_data"
NE_COASTLINE_FILE = NATURAL_EARTH_DATA_DIR / "ne_10m_coastline.json"

logger = logging.getLogger(f"{__name__}")


class NaturalEarthProperties(BaseModel):
    """A model for parsing Natural Earth GeoJSON properties."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    scalerank: int
    featurecla: str
    min_zoom: float | None = Field(
        default=None, validation_alias=AliasChoices("min_zooom", "min_zoom")
    )
    name: str | None = None
    namealt: str | None = None
    region: str | None = None
    subregion: str | None = None
    min_label: float | None = None
    max_label: float | None = None


NaturalEarthFeatureCollection = FeatureCollection[NaturalEarthProperties]


def download_coastlines_file() -> None:
    """Downloads a file describing all the coastlines of the world.

    This file is not part of this repository so it's downloaded from
    github.com/martynafford/natural-earth-geojson
    """
    if NE_COASTLINE_FILE.exists():
        print("Coastline file already downloaded")  # noqa: T201
    else:
        print("Downloading coastline file")  # noqa: T201
        NATURAL_EARTH_DATA_DIR.mkdir(exist_ok=True)

        # Download the NE_COASTLINE_FILE
        urllib.request.urlretrieve(
            "https://raw.githubusercontent.com/martynafford/natural-earth-geojson/refs/heads/master/10m/physical/ne_10m_coastline.json",
            NE_COASTLINE_FILE,
        )


@lru_cache(None)
def _get_coastlines() -> GeometryCollection:
    if not NE_COASTLINE_FILE.exists():
        raise Exception(
            "The coastline file has not been downloaded. Run 'natural-language-geocoding init'."
        )
    with NE_COASTLINE_FILE.open() as f:
        _feature_coll_coasts = NaturalEarthFeatureCollection.model_validate(json.load(f))

    return GeometryCollection([f.geometry for f in _feature_coll_coasts.features])


######################
# Public Functions


@timed_function(logger)
def coastline_of(g: BaseGeometry) -> BaseGeometry | None:
    """Given a geometry finds the area that intersects with a coastline."""
    buffered_geom = add_buffer(g, 2)
    intersection = buffered_geom.intersection(_get_coastlines())
    if intersection.is_empty:
        return None
    return intersection
