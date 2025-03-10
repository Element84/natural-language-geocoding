import os
from collections.abc import Generator, Iterator
from enum import Enum
from pathlib import Path

from e84_geoai_common.geojson import Feature
from pydantic import BaseModel, ConfigDict, Field

from natural_language_geocoding.geocode_db.geocode_db import GeocodeDB, GeoPlace

# ruff: noqa: D103,T201,BLE001,FIX002
# TODO reenable this


class WhosOnFirstPlaceType(Enum):
    microhood = "microhood"
    dependency = "dependency"
    nation = "nation"
    installation = "installation"
    building = "building"
    metroarea = "metroarea"
    arcade = "arcade"
    intersection = "intersection"
    ocean = "ocean"
    marinearea = "marinearea"
    wing = "wing"
    macrocounty = "macrocounty"
    locality = "locality"
    macroregion = "macroregion"
    disputed = "disputed"
    postalcode = "postalcode"
    address = "address"
    timezone = "timezone"
    macrohood = "macrohood"
    continent = "continent"
    localadmin = "localadmin"
    campus = "campus"
    neighbourhood = "neighbourhood"
    planet = "planet"
    custom = "custom"
    empire = "empire"
    venue = "venue"
    marketarea = "marketarea"
    concourse = "concourse"
    region = "region"
    postalregion = "postalregion"
    borough = "borough"
    country = "country"
    county = "county"
    enclosure = "enclosure"


class WhosOnFirstPlaceProperties(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore", frozen=True)

    name: str = Field(validation_alias="wof:name")
    placetype: WhosOnFirstPlaceType = Field(validation_alias="wof:placetype")


class WhosOnFirstFeature(Feature[WhosOnFirstPlaceProperties]):
    id: int


def wof_feature_to_geoplace(wof: WhosOnFirstFeature) -> GeoPlace:
    return GeoPlace(
        name=wof.properties.name,
        type=wof.properties.placetype.value,
        geom=wof.geometry,
    )


def find_all_wof_features(source_dir: Path) -> Generator[WhosOnFirstFeature, None, None]:
    for child in source_dir.iterdir():
        if child.is_dir():
            yield from find_all_wof_features(child)
        elif "-alt-" in child.name:
            print("Ignoring alt file", child)
        elif child.suffix != ".geojson":
            print("Ignoring non-geojson", child)
        else:
            with child.open() as f:
                try:
                    wof = WhosOnFirstFeature.model_validate_json(f.read())
                    yield wof
                except Exception as e:
                    raise Exception(f"Failed loading {child}") from e


def chunk_items[T](seq: Iterator[T], chunk_size: int) -> Generator[list[T], None, None]:
    chunk: list[T] = []
    for item in seq:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if len(chunk) > 0:
        yield chunk


importdir = Path("temp/whosonfirst-data-country-latest")

conn_str = os.getenv("GEOCODE_DB_CONN_STR")
if conn_str is None:
    raise Exception("GEOCODE_DB_CONN_STR must be set")
db = GeocodeDB(conn_str)



for features in chunk_items(find_all_wof_features(importdir), 10):
    places = [wof_feature_to_geoplace(f) for f in features]
    db.insert_geoplaces(places)
    print(f"Inserted {len(features)} features", ','.join([p.name for p in places]))
