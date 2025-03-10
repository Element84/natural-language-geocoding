import json
import os
import tarfile
from collections.abc import Callable, Generator, Iterator
from enum import Enum
from math import ceil
from pathlib import Path
from time import time
from typing import TypeVar

import requests
from e84_geoai_common.geojson import Feature
from pydantic import BaseModel, ConfigDict, Field

from natural_language_geocoding.geocode_db.geocode_db import GeocodeDB, GeoPlace

# ruff: noqa: D103,T201,BLE001,FIX002,ERA001,E501
# TODO reenable this


TEMP_DIR = Path("temp")


def chunk_items[T](seq: Iterator[T], chunk_size: int) -> Generator[list[T], None, None]:
    chunk: list[T] = []
    for item in seq:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if len(chunk) > 0:
        yield chunk


class WhosOnFirstPlaceType(Enum):
    address = "address"
    arcade = "arcade"
    borough = "borough"
    building = "building"
    campus = "campus"
    concourse = "concourse"
    continent = "continent"
    country = "country"
    county = "county"
    custom = "custom"
    dependency = "dependency"
    disputed = "disputed"
    empire = "empire"
    enclosure = "enclosure"
    installation = "installation"
    intersection = "intersection"
    localadmin = "localadmin"
    locality = "locality"
    macrocounty = "macrocounty"
    macrohood = "macrohood"
    macroregion = "macroregion"
    marinearea = "marinearea"
    marketarea = "marketarea"
    metroarea = "metroarea"
    microhood = "microhood"
    nation = "nation"
    neighbourhood = "neighbourhood"
    ocean = "ocean"
    planet = "planet"
    postalcode = "postalcode"
    postalregion = "postalregion"
    region = "region"
    timezone = "timezone"
    venue = "venue"
    wing = "wing"


DOWNLOADABLE_PLACETYPES = [
    WhosOnFirstPlaceType.borough,  # (5.8 MB)
    WhosOnFirstPlaceType.continent,  # (5.0 MB)
    WhosOnFirstPlaceType.country,  # (202.4 MB)
    WhosOnFirstPlaceType.county,  # (563.1 MB)
    WhosOnFirstPlaceType.dependency,  # (1.2 MB)
    WhosOnFirstPlaceType.disputed,  # (1.5 MB)
    WhosOnFirstPlaceType.empire,  # (1.6 MB)
    # TODO temporarily skipping these since they're really big
    # WhosOnFirstPlaceType.localadmin,  # (948.3 MB)
    # WhosOnFirstPlaceType.locality,  # (1.96 GB)
    WhosOnFirstPlaceType.macrocounty,  # (23.7 MB)
    WhosOnFirstPlaceType.macrohood,  # (8.7 MB)
    WhosOnFirstPlaceType.macroregion,  # (32.9 MB)
    WhosOnFirstPlaceType.marinearea,  # (4.6 MB)
    WhosOnFirstPlaceType.marketarea,  # (12.5 MB)
    WhosOnFirstPlaceType.microhood,  # (5.6 MB)
    WhosOnFirstPlaceType.neighbourhood,  # (412.3 MB)
    WhosOnFirstPlaceType.ocean,  # (110 KB)
    WhosOnFirstPlaceType.postalregion,  # (49.5 MB)
    WhosOnFirstPlaceType.region,  # (259.7 MB)
    # We won't download these
    # WhosOnFirstPlaceType.planet (3 KB)
    # WhosOnFirstPlaceType.campus  (72.2 MB)
    # WhosOnFirstPlaceType.timezone  (14.0 MB)
]


class WhosOnFirstPlaceProperties(BaseModel):
    model_config = ConfigDict(
        strict=True,
        extra="allow",
        frozen=True,
        json_encoders={WhosOnFirstPlaceType: lambda x: x.value},
    )

    name: str = Field(validation_alias="wof:name")
    placetype: WhosOnFirstPlaceType = Field(validation_alias="wof:placetype")
    edtf_deprecated: str | None = Field(
        validation_alias="edtf:deprecated",
        default=None,
        description="Appears to indicate when place was deprecated",
    )


class WhosOnFirstFeature(Feature[WhosOnFirstPlaceProperties]):
    id: int

    @property
    def is_deprecated(self) -> bool:
        return self.properties.edtf_deprecated is not None


def wof_feature_to_geoplace(wof: WhosOnFirstFeature) -> GeoPlace:
    return GeoPlace(
        name=wof.properties.name,
        type=wof.properties.placetype.value,
        geom=wof.geometry,
        properties=wof.properties.model_dump(mode="json"),
    )


def _download_placetype(place_type: WhosOnFirstPlaceType) -> Path:
    filename = f"whosonfirst-data-{place_type.value}-latest.tar.bz2"
    place_type_file = TEMP_DIR / filename

    if place_type_file.exists():
        # Return file if it already is downloaded
        return place_type_file
    url = f"https://data.geocode.earth/wof/dist/legacy/{filename}"

    # Download file
    print(f"Downloading {url}")
    response = requests.get(url, stream=True, timeout=10)
    with place_type_file.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return place_type_file


def find_all_geojson_features_files(tar: tarfile.TarFile) -> Generator[tarfile.TarInfo, None, None]:
    for member in tar.getmembers():
        if "-alt-" not in member.name and member.name.endswith(".geojson"):
            yield member


def find_all_wof_features(source_tar: Path) -> Generator[WhosOnFirstFeature, None, None]:
    print("Opening tar", source_tar)
    with tarfile.open(source_tar, "r:bz2") as tar:
        for member in find_all_geojson_features_files(tar):
            f = tar.extractfile(member)
            if f is not None:
                try:
                    yield WhosOnFirstFeature.model_validate_json(f.read())
                except Exception as e:
                    raise Exception(f"Failed loading {member.name}") from e


T = TypeVar("T")


def counting_generator(items: Iterator[T], *, log_after_secs: int = 10) -> Generator[T, None, None]:
    start_time = time()
    last_logged = time()
    for index, item in enumerate(items):
        yield item
        now = time()

        if now - last_logged >= log_after_secs:
            elapsed = now - start_time
            count = index + 1
            rate_per_sec = count / elapsed
            rate_per_min = rate_per_sec * 60
            print(
                f"Processed {count} items. Rate: {ceil(rate_per_min)} per min. Elapsed time: {ceil(elapsed / 60)} mins"
            )
            last_logged = time()


def filter_items(items: Iterator[T], filter_fn: Callable[[T], bool]) -> Generator[T, None, None]:
    for item in items:
        if filter_fn(item):
            yield item


def process_placetypes() -> None:
    conn_str = os.getenv("GEOCODE_DB_CONN_STR")
    if conn_str is None:
        raise Exception("GEOCODE_DB_CONN_STR must be set")
    print("Connecting with", conn_str)
    db = GeocodeDB(conn_str)
    placetype_to_count: dict[str, int] = {}

    for placetype in DOWNLOADABLE_PLACETYPES:
        placetype_to_count[placetype.value] = 0
        placetype_file = _download_placetype(placetype)

        features_iter = find_all_wof_features(placetype_file)
        features_iter = counting_generator(features_iter)
        features_iter = filter_items(features_iter, filter_fn=lambda f: not f.is_deprecated)

        for features in chunk_items(features_iter, 10):
            try:
                places = [wof_feature_to_geoplace(f) for f in features]
                db.insert_geoplaces(places)
            except:
                print("failed places:")
                print(json.dumps([f.model_dump(mode="json") for f in features], indent=2))
                raise
            placetype_to_count[placetype.value] += len(places)
        print(f"Finished with {placetype_to_count[placetype.value]}")


def count_places() -> None:
    placetype_to_count: dict[str, int] = {}

    for placetype in DOWNLOADABLE_PLACETYPES:
        placetype_to_count[placetype.value] = 0
        placetype_file = _download_placetype(placetype)
        with tarfile.open(placetype_file, "r:bz2") as tar:
            for _ in find_all_geojson_features_files(tar):
                placetype_to_count[placetype.value] += 1
    print(placetype_to_count)


process_placetypes()


# placetype = DOWNLOADABLE_PLACETYPES[0]
# placetype_file = _download_placetype(placetype)


# features_iter = find_all_wof_features(placetype_file)
# features_iter = counting_generator(features_iter)
# features_iter = filter_items(features_iter, filter_fn=lambda f: not f.is_deprecated)


# for f in features_iter:
#     sleep(1)
#     print(f.properties.name)
