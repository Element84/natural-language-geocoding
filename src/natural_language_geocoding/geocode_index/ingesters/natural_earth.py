"""TODO document this module."""

import json
import logging
from collections.abc import Generator
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import ClassVar, Literal, Self

import requests
from e84_geoai_common.geojson import Feature
from pydantic import BaseModel, ConfigDict, Field, field_validator

from natural_language_geocoding.geocode_index.geoplace import (
    GeoPlace,
    GeoPlaceSource,
    GeoPlaceSourceType,
    GeoPlaceType,
)
from natural_language_geocoding.geocode_index.index import GeocodeIndex
from natural_language_geocoding.geocode_index.ingesters.hierarchy_finder import get_hierarchies
from natural_language_geocoding.geocode_index.ingesters.ingest_utils import process_ingest_items

_GITHUB_RAW_ROOT = "https://raw.githubusercontent.com/martynafford/natural-earth-geojson"

logger = logging.getLogger(__name__)

_LOCAL_TEMP_DIR = Path("temp")


class _NESourceFile(BaseModel):
    """TODO docs."""

    model_config = ConfigDict(strict=True, extra="allow", frozen=True)

    resolution: Literal["10m", "110m", "50m"] = "10m"
    area_type: Literal["cultural", "physical"]
    name: str

    @property
    def filename(self) -> str:
        return f"ne_{self.resolution}_{self.name}.json"

    @property
    def url(self) -> str:
        return (
            _GITHUB_RAW_ROOT
            + f"/refs/heads/master/{self.resolution}/{self.area_type}/{self.filename}"
        )

    @property
    def local_path(self) -> Path:
        return _LOCAL_TEMP_DIR / self.filename

    def download(self) -> None:
        local_file = self.local_path

        if not local_file.exists():
            # Download file
            logger.info("Downloading %s", self.url)
            response = requests.get(self.url, stream=True, timeout=10)
            with local_file.open("wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)


# These are the source files from natural earth that we'll use.

_NE_AIRPORTS = _NESourceFile(area_type="cultural", name="airports")
_NE_PORTS = _NESourceFile(area_type="cultural", name="ports")
_NE_PARKS_AND_PROTECTED_LANDS_AREA = _NESourceFile(
    area_type="cultural", name="parks_and_protected_lands_area"
)
_NE_LAKES = _NESourceFile(area_type="physical", name="lakes")
_NE_LAKES_EUROPE = _NESourceFile(area_type="physical", name="lakes_europe")
_NE_LAKES_NORTH_AMERICA = _NESourceFile(area_type="physical", name="lakes_north_america")
_NE_RIVERS_EUROPE = _NESourceFile(area_type="physical", name="rivers_europe")
_NE_RIVERS_NORTH_AMERICA = _NESourceFile(area_type="physical", name="rivers_north_america")


_NE_SOURCE_FILES = [
    _NE_AIRPORTS,
    _NE_PORTS,
    _NE_PARKS_AND_PROTECTED_LANDS_AREA,
    _NE_LAKES,
    _NE_LAKES_EUROPE,
    _NE_LAKES_NORTH_AMERICA,
    _NE_RIVERS_EUROPE,
    _NE_RIVERS_NORTH_AMERICA,
]


# TODO move this into a common
class EnumWithValueLookup(Enum):
    """Enum subclass with built-in fast value lookup capability.

    Uses a cached lookup dictionary for O(1) performance.
    """

    # Class variable to store the lookup dictionary
    _value_lookup: ClassVar[dict[str, Self]]

    @classmethod
    def from_value(cls, value: str) -> Self | None:
        """Find enum member by its value using efficient dictionary lookup.

        Args:
            value: The string value to look up

        Returns:
            The enum member if found, None otherwise
        """
        # Initialize the lookup dictionary if it doesn't exist
        if not hasattr(cls, "_value_lookup"):
            cls._value_lookup = {member.value: member for member in cls}

        return cls._value_lookup.get(value)


class _NEPlaceType(EnumWithValueLookup):
    """TODO docs."""

    airport = "Airport"
    alkaline_lake = "Alkaline Lake"
    lake = "Lake"
    lake_centerline = "Lake Centerline"
    national_park_service = "National Park Service"
    port = "Port"
    reservoir = "Reservoir"
    river = "River"

    @classmethod
    def from_feature_cla(cls, feature_cla: str) -> "_NEPlaceType | None":
        if feature_cla.startswith("Intermittent") or feature_cla.endswith("Intermittent"):
            feature_cla = feature_cla.replace("Intermittent", "").strip()
        elif feature_cla.endswith("(Intermittent)"):
            feature_cla = feature_cla.replace("(Intermittent)", "").strip()
        return cls.from_value(feature_cla)

    def to_geoplace_type(self) -> GeoPlaceType:
        _place_type_map = {
            _NEPlaceType.airport: GeoPlaceType.airport,
            _NEPlaceType.alkaline_lake: GeoPlaceType.lake,
            _NEPlaceType.lake: GeoPlaceType.lake,
            _NEPlaceType.lake_centerline: GeoPlaceType.lake,
            _NEPlaceType.reservoir: GeoPlaceType.lake,
            _NEPlaceType.national_park_service: GeoPlaceType.national_park,
            _NEPlaceType.port: GeoPlaceType.port,
            _NEPlaceType.river: GeoPlaceType.river,
        }

        if self in _place_type_map:
            return _place_type_map[self]
        raise NotImplementedError(f"Missing mapping from NEPlaceType to GeoPlaceType for {self}")


class _NEPlaceProperties(BaseModel):
    """TODO docs."""

    model_config = ConfigDict(strict=True, extra="allow", frozen=True)

    # TODO add the additional name fields I found

    name: str | None = None
    name_abb: str | None = None
    name_alt: str | None = None

    featurecla: str

    @field_validator("featurecla")
    @classmethod
    def validate_featurecla(cls, v: str) -> str:
        place_type = _NEPlaceType.from_feature_cla(v)
        if place_type is None:
            raise ValueError(f"Unknown feature class: {v}")
        return v

    @cached_property
    def place_type(self) -> _NEPlaceType:
        # This will always be valid because of the validator above
        place_type = _NEPlaceType.from_feature_cla(self.featurecla)
        if place_type is None:
            raise ValueError(f"Unknown feature class: {self.featurecla}")
        return place_type

    @property
    def feature_name(self) -> str | None:
        return self.name or self.name_abb or self.name_alt

    def get_alternate_names(self) -> list[str]:
        return [name for name in [self.name_abb, self.name_alt] if name is not None]


class _NEFeature(Feature[_NEPlaceProperties]):
    """TODO docs."""

    id: str = Field(
        description=(
            "Unique identifier for a natural earth feature. "
            "Note that these don't natively have them so we have to generate them."
        )
    )


def _get_ne_features_from_source(
    source_idx: int, source: _NESourceFile
) -> Generator[_NEFeature, None, None]:
    """TODO docs."""
    source.download()

    with source.local_path.open() as f:
        features_collection = json.load(f)

    res = source.resolution[0:-1]
    area_letter = source.area_type[0]
    source_file_id = f"ne_{res}{area_letter}{source_idx + 1}"

    for index, feature in enumerate(features_collection["features"]):
        # Example id we'll generate for a feature of 10m cultural for the 5th file and
        # ne_10c5_45
        feature["id"] = f"{source_file_id}_{index + 1}"
        yield _NEFeature.model_validate(feature)


def _ne_feature_to_geoplace(source: _NESourceFile, feature: _NEFeature) -> GeoPlace:
    """TODO docs."""
    props = feature.properties
    name = props.feature_name

    if name is None:
        raise Exception(f"Unexpected feature without name {feature}")

    return GeoPlace(
        id=feature.id,
        place_name=name,
        type=props.place_type.to_geoplace_type(),
        geom=feature.geometry,
        properties=props.model_dump(mode="json"),
        source=GeoPlaceSource(
            source_type=GeoPlaceSourceType.ne,
            source_path=source.url,
        ),
        alternate_names=props.get_alternate_names(),
    )


def _get_all_ne_features() -> Generator[tuple[_NESourceFile, _NEFeature], None, None]:
    """TODO docs."""
    for source_idx, source in enumerate(_NE_SOURCE_FILES):
        for feature in _get_ne_features_from_source(source_idx, source):
            if (
                feature.properties.feature_name is not None
                and feature.properties.place_type != _NEPlaceType.lake_centerline
            ):
                yield (source, feature)


def _bulk_index_features(
    index: GeocodeIndex, source_features: list[tuple[_NESourceFile, _NEFeature]]
) -> None:
    """TODO docs."""
    places: list[GeoPlace] = []
    for source, feature in source_features:
        place = _ne_feature_to_geoplace(source, feature)
        hierarchies = get_hierarchies(index, place)
        place = GeoPlace.model_validate({**place.model_dump(), "hierarchies": hierarchies})
        places.append(place)
    index.bulk_index(places)


def process_features() -> None:
    """TODO docs."""
    process_ingest_items(_get_all_ne_features(), _bulk_index_features)


## Code for manual testing
# ruff: noqa: ERA001,T201,E402

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
# )
