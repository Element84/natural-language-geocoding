"""Helpers for dealing with GeoJSON from Natural Earth"""

from functools import lru_cache
import json
import os
import urllib.request

from e84_geoai_common.geojson import FeatureCollection
from e84_geoai_common.geometry import add_buffer
from e84_geoai_common.util import timed_function
from pydantic import AliasChoices, BaseModel, ConfigDict, Field
from shapely import GeometryCollection
from shapely.geometry.base import BaseGeometry

NATURAL_EARTH_DATA_DIR = os.path.join(os.path.dirname(__file__), "natural_earth_data")
NE_COASTLINE_FILE = os.path.join(NATURAL_EARTH_DATA_DIR, "ne_10m_coastline.json")


class NaturalEarthProperties(BaseModel):
    """A model for parsing Natural Earth GeoJSON properties"""

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


def download_coastlines_file():
    """
    Downloads a file describing all the coastlines of the world.

    This file is not part of this repository so it's downloaded from github.com/martynafford/natural-earth-geojson
    """
    if os.path.exists(NE_COASTLINE_FILE):
        print("Coastline file already downloaded")
    else:
        print("Downloading coastline file")
        os.makedirs(NATURAL_EARTH_DATA_DIR, exist_ok=True)

        # Download the NE_COASTLINE_FILE
        url = "https://raw.githubusercontent.com/martynafford/natural-earth-geojson/refs/heads/master/10m/physical/ne_10m_coastline.json"
        urllib.request.urlretrieve(url, NE_COASTLINE_FILE)


@lru_cache(None)
def _get_coastlines() -> GeometryCollection:
    if not os.path.exists(NE_COASTLINE_FILE):
        raise Exception(
            "The coastline file has not been downloaded. Run 'natural-language-geocoding init'."
        )
    with open(NE_COASTLINE_FILE) as f:
        _feature_coll_coasts = NaturalEarthFeatureCollection.model_validate(
            json.load(f)
        )

    return GeometryCollection([f.geometry for f in _feature_coll_coasts.features])


######################
# Public Functions


@timed_function
def coastline_of(g: BaseGeometry) -> BaseGeometry | None:
    """Given a geometry finds the area that intersects with a coastline."""
    buffered_geom = add_buffer(g, 2)
    intersection = buffered_geom.intersection(_get_coastlines())
    if intersection.is_empty:
        return None
    else:
        return intersection
