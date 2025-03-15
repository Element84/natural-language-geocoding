import json
import os
import tarfile
import threading
from collections.abc import Callable, Generator, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
from math import ceil
from pathlib import Path
from time import time
from typing import TypeVar

import requests
from e84_geoai_common.geojson import Feature
from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from natural_language_geocoding.geocode_index.geoplace import (
    GeoPlace,
    GeoPlaceSource,
    GeoPlaceSourceType,
    GeoPlaceType,
    Hierarchy,
)
from natural_language_geocoding.geocode_index.index import GeocodeIndex

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


# Documentation copied from https://whosonfirst.org/docs/placetypes/


class WhosOnFirstPlaceType(Enum):
    address = "address"
    arcade = "arcade"
    borough = "borough"
    building = "building"

    # Things like universities or office complexes and airports.
    campus = "campus"
    concourse = "concourse"
    continent = "continent"

    # Basically places that issue passports, notwithstanding the details (like empires which actually issue the passports...)
    country = "country"
    county = "county"

    custom = "custom"

    # It's not a sub-region of a country but rather dependent on a parent country for defence, passport control, subsidies, etc
    dependency = "dependency"

    # Places that one or more parties claim as their own. As of this writing all disputed places are parented only by the country (and higher) IDs of the claimants. This isn't to say there aren't more granular hierarchies to be applied to these place only that we are starting with the simple stuff first.
    disputed = "disputed"

    # Or "sovereignty" but really... empire. For example the Meta United States that contains both the US and Puerto Rico.
    empire = "empire"

    enclosure = "enclosure"
    installation = "installation"
    intersection = "intersection"

    # In many countries, the lowest level of government. They contain one or more localities (or "populated places") which themselves have no authority. Often but not exclusively found in Europe.
    localadmin = "localadmin"

    # Towns and cities, independent of size or population. Things with neighbourhoods, basically.
    locality = "locality"

    # Macrocounties are considered optional. These exists mostly in Europe.
    macrocounty = "macrocounty"

    # Like "BoCoCa" which in WOE is a neighbourhood that parents another... neighbourhood.
    macrohood = "macrohood"

    # Bundles of regions! These exists mostly in Europe.
    macroregion = "macroregion"

    # Places with fish and boats.
    marinearea = "marinearea"
    marketarea = "marketarea"

    # Things like "The Bay Area" – this one is hard so we shouldn't spend too much time worrying about the details yet but instead treat as something we want to do eventually.
    metroarea = "metroarea"

    microhood = "microhood"

    nation = "nation"
    neighbourhood = "neighbourhood"
    ocean = "ocean"
    planet = "planet"
    postalcode = "postalcode"
    postalregion = "postalregion"

    # States, provinces, regions. We call them regions
    region = "region"
    timezone = "timezone"

    # Things with walls, often but mostly things that people stand around together. Things with walls might be public (a bar) or private (your apartment) by default.
    venue = "venue"
    wing = "wing"

    def to_geoplace_type(self) -> GeoPlaceType:
        return GeoPlaceType(self.value)


DOWNLOADABLE_PLACETYPES = [
    WhosOnFirstPlaceType.borough,  # (5.8 MB)
    WhosOnFirstPlaceType.continent,  # (5.0 MB)
    WhosOnFirstPlaceType.country,  # (202.4 MB)
    WhosOnFirstPlaceType.county,  # (563.1 MB)
    WhosOnFirstPlaceType.dependency,  # (1.2 MB)``
    WhosOnFirstPlaceType.disputed,  # (1.5 MB)
    WhosOnFirstPlaceType.empire,  # (1.6 MB)
    WhosOnFirstPlaceType.localadmin,  # (948.3 MB)
    WhosOnFirstPlaceType.locality,  # (1.96 GB)
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


class WofHierarchy(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    continent_id: int | None = None
    empire_id: int | None = None
    country_id: int | None = None
    locality_id: int | None = None
    macroregion_id: int | None = None
    region_id: int | None = None


class WhosOnFirstPlaceProperties(BaseModel):
    model_config = ConfigDict(
        strict=True,
        extra="allow",
        frozen=True,
        json_encoders={WhosOnFirstPlaceType: lambda x: x.value},
    )

    name: str | None = Field(validation_alias=AliasChoices("name", "wof:name"))
    placetype: WhosOnFirstPlaceType = Field(validation_alias="wof:placetype")
    edtf_deprecated: str | None = Field(
        validation_alias="edtf:deprecated",
        default=None,
        description="Appears to indicate when place was deprecated",
    )
    # TODO add in alternative spellings
    # name:eng_x_...
    hierarchies: list[WofHierarchy] = Field(validation_alias="wof:hierarchy", default_factory=list)


class WhosOnFirstFeature(Feature[WhosOnFirstPlaceProperties]):
    id: int

    @property
    def is_deprecated(self) -> bool:
        return self.properties.edtf_deprecated is not None


def _wof_feature_to_geoplace(feature: WhosOnFirstFeature, source_path: str) -> GeoPlace:
    name = feature.properties.name
    if name is None:
        raise Exception(f"Can't convert feature [{feature.id}] to geoplace without a name.")

    if len(feature.properties.hierarchies) > 0:
        if len(feature.properties.hierarchies) > 1:
            raise Exception(f"Can't handle feature [{feature.id}] with multiple hierarchies")
        wof_h = feature.properties.hierarchies[0]
        hierarchy = Hierarchy(
            continent_id=wof_h.continent_id,
            country_id=wof_h.country_id,
            locality_id=wof_h.locality_id,
            macroregion_id=wof_h.macroregion_id,
            region_id=wof_h.region_id,
        )
    else:
        hierarchy = None

    return GeoPlace(
        id=f"wof_{feature.id}",
        name=name,
        type=feature.properties.placetype.to_geoplace_type(),
        geom=feature.geometry,
        properties=feature.properties.model_dump(mode="json"),
        source=GeoPlaceSource(
            source_type=GeoPlaceSourceType.wof,
            source_path=source_path,
        ),
        source_id=feature.id,
        hierarchy=hierarchy,
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
K = TypeVar("K")


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


def filter_items(
    items: Iterator[T], filter_fn: Callable[[T], bool], *, log_not_matching: bool = False
) -> Generator[T, None, None]:
    for item in items:
        if filter_fn(item):
            yield item
        elif log_not_matching:
            print("Filtered out", item)


def unique_by(items: Iterator[T], key_fn: Callable[[T], K]) -> Iterator[T]:
    keys: set[K] = set()

    for item in items:
        key = key_fn(item)
        if key in keys:
            print("Skipping duplicate key", key)
        else:
            keys.add(key)
            yield item


def process_placetype_file(index: GeocodeIndex, placetype_file: Path) -> None:
    features_iter = find_all_wof_features(placetype_file)
    features_iter = counting_generator(features_iter)
    features_iter = filter_items(features_iter, filter_fn=lambda f: not f.is_deprecated)
    features_iter = filter_items(
        features_iter,
        filter_fn=lambda f: f.properties.name is not None,
        log_not_matching=True,
    )
    # Who's on first places are sometimes duplicated with similar information.
    features_iter = unique_by(features_iter, lambda p: p.id)
    count = 0

    for features in chunk_items(features_iter, 50):
        try:
            places = [_wof_feature_to_geoplace(f, placetype_file.name) for f in features]
            index.bulk_index(places)
        except:
            print("failed places:")
            print(json.dumps([f.model_dump(mode="json") for f in features], indent=2))
            raise
        count += len(places)
    print(f"Finished {placetype_file} and saved {count}")


def process_placetype_file_multithread(placetype_file: Path) -> None:
    features_iter = find_all_wof_features(placetype_file)
    features_iter = counting_generator(features_iter)
    features_iter = filter_items(features_iter, filter_fn=lambda f: not f.is_deprecated)
    features_iter = filter_items(
        features_iter,
        filter_fn=lambda f: f.properties.name is not None,
        log_not_matching=True,
    )
    # Who's on first places are sometimes duplicated with similar information.
    features_iter = unique_by(features_iter, lambda p: p.id)

    thread_local = threading.local()
    all_conns: set[GeocodeIndex] = set()

    def _get_index() -> GeocodeIndex:
        if not hasattr(thread_local, "index"):
            thread_local.index = GeocodeIndex()
            all_conns.add(thread_local.index)

        return thread_local.index

    def _bulk_index(features: list[WhosOnFirstFeature]) -> None:
        index = _get_index()
        try:
            places = [_wof_feature_to_geoplace(f, placetype_file.name) for f in features]
            index.bulk_index(places)
        except:
            print("failed places:")
            print(json.dumps([f.model_dump(mode="json") for f in features], indent=2))
            raise

    with ThreadPoolExecutor(max_workers=5) as e:
        futures = [e.submit(_bulk_index, features) for features in chunk_items(features_iter, 50)]
        for future in as_completed(futures):
            future.result()
        for conn in all_conns:
            conn.client.close()


def process_placetypes() -> None:
    conn_str = os.getenv("GEOCODE_DB_CONN_STR")
    if conn_str is None:
        raise Exception("GEOCODE_DB_CONN_STR must be set")
    index = GeocodeIndex()

    for placetype in DOWNLOADABLE_PLACETYPES:
        placetype_file = _download_placetype(placetype)
        process_placetype_file(index, placetype_file)


# if __name__ == "__main__":
#     process_placetypes()

placetype_file = Path("temp/whosonfirst-data-country-latest.tar.bz2")

index = GeocodeIndex()
index.create_index(recreate=True)
# process_placetype_file(index, placetype_file)

# Initial 159 per minute (10 per bulk request, default refresh, default number of replicas)
# 170 per minute (10 per bulk request, refresh 30s, 0 replicas)
# 187 per minute (50 per bulk request, refresh 30s, 0 replicas)

process_placetype_file_multithread(placetype_file)

# 210 per minute (Multithreaded 5 threads, 50 per bulk request, refresh 30s, 0 replicas)
