"""Provides functions for indexing data from Who's On First.

[Who's On First](https://whosonfirst.org/) is an online distribution of places on the earth with
information about each place like its spatial geometry, name, type, and hierarchy. See
https://whosonfirst.org/docs/licenses/ for licensing information.

The general process for indexing Who's On First (WOF) data is as follows:

1. Download source tar files from WOF to a local directory. Each source tar is a `.tar.bz2` file.
2. Read all of the Geojson features from each source tar. Each feature is in a separate file in the
tar.

"""

import logging
import tarfile
from collections.abc import Generator, Iterable, Sequence
from enum import Enum
from pathlib import Path
from typing import Any, cast

import requests
from e84_geoai_common.geojson import Feature
from e84_geoai_common.util import unique_by
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

from natural_language_geocoding.geocode_index.geoplace import (
    GeoPlace,
    GeoPlaceSource,
    GeoPlaceSourceType,
    GeoPlaceType,
    Hierarchy,
)
from natural_language_geocoding.geocode_index.index import GeocodeIndex
from natural_language_geocoding.geocode_index.ingesters.ingest_utils import (
    counting_generator,
    filter_items,
    fix_geometry,
    process_ingest_items,
)

_LOCAL_TEMP_DIR = Path("temp")

logger = logging.getLogger(__name__)


_KNOWN_BAD_WOF_GEOMS__: set[int] = {
    # Pontes e Lacerda, a small town in Brazil. Opensearch rejects the polygon's 40th interior ring.
    101961007
}


class _WhosOnFirstPlaceType(Enum):
    """Defines all the different kinds of WOF placetypes."""

    # Documentation in comments copied from https://whosonfirst.org/docs/placetypes/

    address = "address"
    arcade = "arcade"
    borough = "borough"
    building = "building"

    # Things like universities or office complexes and airports.
    campus = "campus"
    concourse = "concourse"
    continent = "continent"

    # Basically places that issue passports, notwithstanding the details (like empires which
    # actually issue the passports...)
    country = "country"
    county = "county"

    custom = "custom"

    # It's not a sub-region of a country but rather dependent on a parent country for defence,
    # passport control, subsidies, etc
    dependency = "dependency"

    # Places that one or more parties claim as their own. As of this writing all disputed places are
    # parented only by the country (and higher) IDs of the claimants. This isn't to say there aren't
    # more granular hierarchies to be applied to these place only that we are starting with the
    # simple stuff first.
    disputed = "disputed"

    # Or "sovereignty" but really... empire. For example the Meta United States that contains both
    # the US and Puerto Rico.
    empire = "empire"

    enclosure = "enclosure"
    installation = "installation"
    intersection = "intersection"

    # In many countries, the lowest level of government. They contain one or more localities (or
    #  "populated places") which themselves have no authority. Often but not exclusively found in
    # Europe.
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

    # Things like "The Bay Area" - this one is hard so we shouldn't spend too much time worrying
    # about the details yet but instead treat as something we want to do eventually.
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

    # Things with walls, often but mostly things that people stand around together. Things with
    # walls might be public (a bar) or private (your apartment) by default.
    venue = "venue"
    wing = "wing"

    def to_geoplace_type(self) -> GeoPlaceType:
        return GeoPlaceType(self.value)


_DOWNLOADABLE_PLACETYPES = [
    _WhosOnFirstPlaceType.borough,  # (5.8 MB) 474 records
    _WhosOnFirstPlaceType.continent,  # (5.0 MB) 8 records
    _WhosOnFirstPlaceType.country,  # (202.4 MB) 232 records
    _WhosOnFirstPlaceType.county,  # (563.1 MB) 47,645 records
    _WhosOnFirstPlaceType.dependency,  # (1.2 MB) 43 records
    _WhosOnFirstPlaceType.disputed,  # (1.5 MB) 104 records
    _WhosOnFirstPlaceType.empire,  # (1.6 MB) 12 records
    _WhosOnFirstPlaceType.localadmin,  # (948.3 MB) 203,541 records
    _WhosOnFirstPlaceType.locality,  # (1.96 GB) 5,053,746 records
    _WhosOnFirstPlaceType.macrocounty,  # (23.7 MB) 581 records
    _WhosOnFirstPlaceType.macrohood,  # (8.7 MB) 1,272 records
    _WhosOnFirstPlaceType.macroregion,  # (32.9 MB) 118 records
    _WhosOnFirstPlaceType.marinearea,  # (4.6 MB) 402 records
    _WhosOnFirstPlaceType.marketarea,  # (12.5 MB) 210 records
    _WhosOnFirstPlaceType.microhood,  # (5.6 MB) 2,287 records
    _WhosOnFirstPlaceType.neighbourhood,  # (412.3 MB) 413,374 records
    _WhosOnFirstPlaceType.ocean,  # (110 KB) 7 records
    _WhosOnFirstPlaceType.postalregion,  # (49.5 MB) 2841 records
    _WhosOnFirstPlaceType.region,  # (259.7 MB) 5315 records
    # We won't download these
    # WhosOnFirstPlaceType.planet (3 KB)
    # WhosOnFirstPlaceType.campus  (72.2 MB)
    # WhosOnFirstPlaceType.timezone  (14.0 MB)
]

_VALID_WOF_HIERARCHY_KEYS = {
    key for place_type in GeoPlaceType for key in [place_type.value, f"{place_type.value}_id"]
}


def _wof_hierarchy_parser(value: Any) -> Any:  # noqa: ANN401
    """Handles parsing hierarchies from Who's on first.

    They're not consistent and sometimes refer to ids with 'field_id' and sometimes 'field' or both.
    This gets the value that is not -1 that's available. It ignores all other keys in the hierarchy
    """
    if isinstance(value, dict):
        value_dict: dict[Any, Any] = cast("dict[Any, Any]", value)

        def _pick_value(hierarchy_field: str) -> str | None:
            id_value = value_dict.get(hierarchy_field)
            key_plain = hierarchy_field.replace("_id", "")
            plain_value = value_dict.get(key_plain)
            if id_value is not None and isinstance(id_value, int) and id_value >= 0:
                return f"wof_{id_value}"
            if plain_value is not None and isinstance(plain_value, int) and plain_value >= 0:
                return f"wof_{plain_value}"
            return None

        return {field: _pick_value(field) for field in Hierarchy.model_fields}

    return value


class _WhosOnFirstPlaceProperties(BaseModel):
    """The Geojson feature properties from a Who's On First feature.

    This only defines the fields that we need.
    """

    model_config = ConfigDict(
        strict=True,
        extra="allow",
        frozen=True,
        # FUTURE this is deprecated in pydantic. Move to a different implementations
        json_encoders={_WhosOnFirstPlaceType: lambda x: x.value},
    )

    name: str | None = Field(validation_alias=AliasChoices("name", "wof:name"))
    placetype: _WhosOnFirstPlaceType = Field(
        validation_alias=AliasChoices("wof:placetype", "placetype")
    )
    edtf_deprecated: str | None = Field(
        validation_alias="edtf:deprecated",
        default=None,
        description="Appears to indicate when place was deprecated",
    )

    eng_x_colloquial: list[str | None] = Field(
        validation_alias="name:eng_x_colloquial", default_factory=list[str | None]
    )
    eng_x_historical: list[str | None] = Field(
        validation_alias="name:eng_x_historical", default_factory=list[str | None]
    )
    eng_x_preferred: list[str | None] = Field(
        validation_alias="name:eng_x_preferred", default_factory=list[str | None]
    )
    eng_x_unknown: list[str | None] = Field(
        validation_alias="name:eng_x_unknown", default_factory=list[str | None]
    )
    eng_x_variant: list[str | None] = Field(
        validation_alias="name:eng_x_variant", default_factory=list[str | None]
    )
    area_square_m: float | None = Field(validation_alias="geom:area_square_m", default=None)
    population: int | None = Field(validation_alias="wof:population", default=None)

    hierarchies: list[Hierarchy] = Field(
        validation_alias="wof:hierarchy", default_factory=list[Hierarchy]
    )

    @field_validator("hierarchies", mode="before")
    @classmethod
    def _wof_hierarchies_parser(cls, value: Any) -> Any:  # noqa: ANN401
        if isinstance(value, list):
            values: list[Any] = cast("list[Any]", value)
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


class _WhosOnFirstFeature(Feature[_WhosOnFirstPlaceProperties]):
    """A Who's On First geojson feature."""

    id: int

    @property
    def is_deprecated(self) -> bool:
        return self.properties.edtf_deprecated is not None


def _wof_feature_to_geoplace(feature: _WhosOnFirstFeature, source_path: str) -> GeoPlace:
    """Converts a WOF feature to a GeoPlace."""
    props = feature.properties
    name = props.name
    if name is None:
        raise Exception(f"Can't convert feature [{feature.id}] to geoplace without a name.")

    return GeoPlace(
        id=f"wof_{feature.id}",
        place_name=name,
        type=props.placetype.to_geoplace_type(),
        geom=fix_geometry(f"wof_{feature.id}", feature.geometry),
        properties=props.model_dump(mode="json"),
        source=GeoPlaceSource(
            source_type=GeoPlaceSourceType.wof,
            source_path=source_path,
        ),
        hierarchies=props.hierarchies,
        alternate_names=props.get_alternate_names(),
        area_sq_km=props.area_square_m / 1000.0 if props.area_square_m else None,
        population=props.population,
    )


def _download_placetype(place_type: _WhosOnFirstPlaceType) -> Path:
    """Downloads the WOF file if it's not already downloaded."""
    filename = f"whosonfirst-data-{place_type.value}-latest.tar.bz2"
    place_type_file = _LOCAL_TEMP_DIR / filename

    if place_type_file.exists():
        # Return file if it already is downloaded
        return place_type_file
    url = f"https://data.geocode.earth/wof/dist/legacy/{filename}"

    # Download file
    logger.info("Downloading %s", url)
    response = requests.get(url, stream=True, timeout=10)
    with place_type_file.open("wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return place_type_file


def _find_all_geojson_features_files(
    tar: tarfile.TarFile,
) -> Generator[tarfile.TarInfo, None, None]:
    """Returns all members of the tar file that are WOF Geojson features."""
    member = tar.next()
    while member is not None:
        # Skip the alternate geometries
        if "-alt-" not in member.name and member.name.endswith(".geojson"):
            yield member
        member = tar.next()


def _find_all_wof_features(placetype_tar_file: Path) -> Generator[_WhosOnFirstFeature, None, None]:
    """Given a WOF tar file, returns every WOF Feature in the tar file."""
    logger.info("Opening tar %s", placetype_tar_file)
    with tarfile.open(placetype_tar_file, "r:bz2") as tar:
        for member in _find_all_geojson_features_files(tar):
            f = tar.extractfile(member)
            if f is not None:
                try:
                    yield _WhosOnFirstFeature.model_validate_json(f.read())
                except Exception as e:
                    raise Exception(f"Failed loading {member.name}") from e


def _placetype_tar_file_to_features_for_ingest(
    placetype_tar_file: Path,
) -> Iterable[_WhosOnFirstFeature]:
    """Returns all of the WOF Features from a tar file that we want to ingest.

    Skips deprecated places, places without names, known bad geometries, etc.
    """
    features_iter = _find_all_wof_features(placetype_tar_file)
    features_iter = counting_generator(features_iter, logger=logger)
    # Exclude deprecated places
    features_iter = filter_items(features_iter, filter_fn=lambda f: not f.is_deprecated)
    # Exclude any places with no name.
    features_iter = filter_items(
        features_iter,
        filter_fn=lambda f: f.properties.name is not None,
        logger=logger,
    )
    # Exclude any places with known bad geometries
    features_iter = filter_items(
        features_iter, filter_fn=lambda f: f.id not in _KNOWN_BAD_WOF_GEOMS__
    )
    # Who's on first places are sometimes duplicated with similar information.
    return unique_by(
        features_iter,
        key_fn=lambda p: p.id,
        duplicate_handler_fn=lambda _f, feature_id: logger.info(
            "Skipping duplicate id %s", feature_id
        ),
    )


def _index_placetype_tar_file(placetype_tar_file: Path) -> None:
    """Indexes all of the features in the tar file."""

    def _bulk_index(index: GeocodeIndex, features: Sequence[_WhosOnFirstFeature]) -> None:
        places = [_wof_feature_to_geoplace(f, placetype_tar_file.name) for f in features]
        index.bulk_index(places)

    features_iter = _placetype_tar_file_to_features_for_ingest(placetype_tar_file)

    process_ingest_items(features_iter, _bulk_index)


def index_wof_places() -> None:
    """Indexes all of the WOF features into the geocode index."""
    index = GeocodeIndex()
    index.create_index(recreate=True)

    for placetype in _DOWNLOADABLE_PLACETYPES:
        placetype_tar_file = _download_placetype(placetype)
        _index_placetype_tar_file(placetype_tar_file)


if __name__ == "__main__" and "get_ipython" not in globals():
    logging.getLogger("opensearch").setLevel(logging.WARNING)

    index_wof_places()


# Code for manual testing
# ruff: noqa: ERA001

# placetype_tar_file = Path("temp/whosonfirst-data-locality-latest.tar.bz2")

# found_features: list[_WhosOnFirstFeature] = []

# with tarfile.open(placetype_tar_file, "r:bz2") as tar:
#     f = tar.extractfile("data/101/736/167/101736167.geojson")
#     if f is not None:
#         feature = _WhosOnFirstFeature.model_validate_json(f.read())

# with open("temp/found_bad_geoms.json.ld", "w") as f:
#     for feature in _placetype_tar_file_to_features_for_ingest(placetype_tar_file):
#         try:
#             fix_geometry(str(feature.id), feature.geometry)
#         except Exception as e:
#             print(e)
#             found_features.append(feature)
#             f.write(feature.model_dump_json())
#             f.write("\n")
#             f.flush()
# print("Done")
