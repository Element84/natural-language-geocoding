import json
import tarfile
import threading
from collections.abc import Callable, Generator, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
from math import ceil
from pathlib import Path
from time import time
from typing import Any, TypeVar

import requests
from e84_geoai_common.geojson import Feature
from e84_geoai_common.util import chunk_items, unique_by
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

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

    # Things like "The Bay Area" - this one is hard so we shouldn't spend too much time worrying about the details yet but instead treat as something we want to do eventually.
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
    # Commenting out already completed placetypes
    WhosOnFirstPlaceType.borough,  # (5.8 MB)
    WhosOnFirstPlaceType.continent,  # (5.0 MB)
    WhosOnFirstPlaceType.country,  # (202.4 MB)
    WhosOnFirstPlaceType.county,  # (563.1 MB)
    WhosOnFirstPlaceType.dependency,  # (1.2 MB)
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

VALID_WOF_HIERARCHY_KEYS = {
    key for place_type in GeoPlaceType for key in [place_type.value, f"{place_type.value}_id"]
}


def _wof_hierarchy_parser(value: Any) -> Any:  # noqa: ANN401
    """Handles parsing hierarchies from Who's on first.

    They're not consistent and sometimes refer to ids with 'field_id' and sometimes 'field' or both.
    This gets the value that is not -1 that's available. It ignores all other keys in the hierarchy
    """
    if isinstance(value, dict):
        value_dict: dict[Any, Any] = value

        def _pick_value(place_type: GeoPlaceType) -> str | None:
            key_plain = place_type.value
            key_id = f"{key_plain}_id"
            plain_value = value_dict.get(key_plain)
            id_value = value_dict.get(key_id)
            if id_value is not None and isinstance(id_value, int) and id_value >= 0:
                return f"wof_{id_value}"
            if plain_value is not None and isinstance(plain_value, int) and plain_value >= 0:
                return f"wof_{plain_value}"
            return None

        return {f"{place_type.value}_id": _pick_value(place_type) for place_type in GeoPlaceType}

    return value


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

    eng_x_colloquial: list[str | None] = Field(
        validation_alias="name:eng_x_colloquial", default_factory=list
    )
    eng_x_historical: list[str | None] = Field(
        validation_alias="name:eng_x_historical", default_factory=list
    )
    eng_x_preferred: list[str | None] = Field(
        validation_alias="name:eng_x_preferred", default_factory=list
    )
    eng_x_unknown: list[str | None] = Field(
        validation_alias="name:eng_x_unknown", default_factory=list
    )
    eng_x_variant: list[str | None] = Field(
        validation_alias="name:eng_x_variant", default_factory=list
    )
    area_square_m: float | None = Field(validation_alias="geom:area_square_m", default=None)
    population: int | None = Field(validation_alias="wof:population", default=None)

    hierarchies: list[Hierarchy] = Field(validation_alias="wof:hierarchy", default_factory=list)

    @field_validator("hierarchies", mode="before")
    @classmethod
    def _wof_hierarchies_parser(cls, value: Any) -> Any:  # noqa: ANN401
        if isinstance(value, list):
            values: list[Any] = value
            return [_wof_hierarchy_parser(item) for item in values]
        return value

    def get_alternate_names(self) -> list[str]:
        return [
            name
            for name in [
                *self.eng_x_colloquial,
                *self.eng_x_historical,
                *self.eng_x_preferred,
                *self.eng_x_unknown,
                *self.eng_x_variant,
            ]
            if name is not None
        ]


class WhosOnFirstFeature(Feature[WhosOnFirstPlaceProperties]):
    id: int

    @property
    def is_deprecated(self) -> bool:
        return self.properties.edtf_deprecated is not None


def _wof_feature_to_geoplace(feature: WhosOnFirstFeature, source_path: str) -> GeoPlace:
    props = feature.properties
    name = props.name
    if name is None:
        raise Exception(f"Can't convert feature [{feature.id}] to geoplace without a name.")

    return GeoPlace(
        id=f"wof_{feature.id}",
        name=name,
        type=props.placetype.to_geoplace_type(),
        geom=feature.geometry,
        properties=props.model_dump(mode="json"),
        source=GeoPlaceSource(
            source_type=GeoPlaceSourceType.wof,
            source_path=source_path,
        ),
        source_id=feature.id,
        hierarchies=props.hierarchies,
        alternate_names=props.get_alternate_names(),
        area_sq_km=props.area_square_m / 1000.0 if props.area_square_m else None,
        population=props.population,
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
    count = 0

    def _log() -> None:
        now = time()
        elapsed = now - start_time
        rate_per_sec = count / elapsed
        rate_per_min = rate_per_sec * 60
        print(
            f"Processed {count} items. Rate: {ceil(rate_per_min)} per min. Elapsed time: {ceil(elapsed / 60)} mins"
        )

    for item in items:
        yield item
        count += 1
        if time() - last_logged >= log_after_secs:
            _log()
            last_logged = time()

    _log()


def filter_items(
    items: Iterator[T], filter_fn: Callable[[T], bool], *, log_not_matching: bool = False
) -> Generator[T, None, None]:
    for item in items:
        if filter_fn(item):
            yield item
        elif log_not_matching:
            print("Filtered out", item)


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
    features_iter = unique_by(
        features_iter,
        key_fn=lambda p: p.id,
        duplicate_handler_fn=lambda _f, feature_id: print(f"Skipping duplicate id {feature_id}"),
    )
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
    features_iter = unique_by(
        features_iter,
        key_fn=lambda p: p.id,
        duplicate_handler_fn=lambda _f, feature_id: print(f"Skipping duplicate id {feature_id}"),
    )

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
    index = GeocodeIndex()
    index.create_index(recreate=True)

    for placetype in DOWNLOADABLE_PLACETYPES:
        placetype_file = _download_placetype(placetype)
        process_placetype_file_multithread(placetype_file)


if __name__ == "__main__":
    import logging

    logging.getLogger("opensearch").setLevel(logging.WARNING)

    process_placetypes()

# Code for manual testing

# placetype_file = Path("temp/whosonfirst-data-country-latest.tar.bz2")

# index = GeocodeIndex()
# index.create_index(recreate=True)
# # process_placetype_file(index, placetype_file)
# process_placetype_file_multithread(placetype_file)

# Initial 159 per minute (10 per bulk request, default refresh, default number of replicas)
# 170 per minute (10 per bulk request, refresh 30s, 0 replicas)
# 187 per minute (50 per bulk request, refresh 30s, 0 replicas)
