"""Helpers for dealing with GeoJSON from Natural Earth"""

import json

from e84_geoai_common.features import FeatureCollection
from e84_geoai_common.geometry import add_buffer
from e84_geoai_common.util import timed_function
from pydantic import AliasChoices, BaseModel, ConfigDict, Field
from shapely import GeometryCollection
from shapely.geometry.base import BaseGeometry

NE_COASTLINE_FILE = "natural_earth_data/ne_10m_coastline.json"
NE_REGIONS_FILE = "natural_earth_data/ne_10m_geography_regions_polys.json"


class NaturalEarthProperties(BaseModel):
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


with open(NE_COASTLINE_FILE) as f:
    _feature_coll_coasts = NaturalEarthFeatureCollection.model_validate(json.load(f))

ALL_COASTLINES = GeometryCollection([f.geometry for f in _feature_coll_coasts.features])

with open(NE_REGIONS_FILE) as f:
    _feature_coll_regions = NaturalEarthFeatureCollection.model_validate(json.load(f))

_abbreviations_to_full = {
    "ra.": "range",
    "pen.": "peninsula",
    "pén.": "peninsula",
    "mts.": "mountains",
    "i.": "island",
    "î.": "island",
    "is.": "island",
    "plat.": "plateau",
    "arch.": "archipelago",
    "cord.": "cordillera",
    "s.": "south",
    "n.": "north",
    "st.": "saint",
}


def _region_name_to_searchable(name: str | None):
    if name is None:
        raise Exception("region name is none")
    name = name.lower()

    for abbrev, full in _abbreviations_to_full.items():
        if abbrev in name:
            name = name.replace(abbrev, full)
    return name


_region_to_geom: dict[str, BaseGeometry] = {
    _region_name_to_searchable(feature.properties.name): feature.geometry
    for feature in _feature_coll_regions.features
}

######################
# Public Functions


@timed_function
def coastline_of(g: BaseGeometry) -> BaseGeometry | None:
    buffered_geom = add_buffer(g, 2)
    intersection = buffered_geom.intersection(ALL_COASTLINES)
    if intersection.is_empty:
        return None
    else:
        return intersection


def find_region_geometry(region_name: str) -> BaseGeometry | None:
    search_name = _region_name_to_searchable(region_name)
    return _region_to_geom.get(search_name)
