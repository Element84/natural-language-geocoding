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

_GITHUB_RAW_ROOT = "https://raw.githubusercontent.com/martynafford/natural-earth-geojson"

logger = logging.getLogger(__name__)

TEMP_DIR = Path("temp")


class NESourceFile(BaseModel):
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
        return TEMP_DIR / self.filename

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

_NE_AIRPORTS = NESourceFile(area_type="cultural", name="airports")
_NE_PORTS = NESourceFile(area_type="cultural", name="ports")
_NE_PARKS_AND_PROTECTED_LANDS_AREA = NESourceFile(
    area_type="cultural", name="parks_and_protected_lands_area"
)
_NE_LAKES = NESourceFile(area_type="physical", name="lakes")
_NE_LAKES_EUROPE = NESourceFile(area_type="physical", name="lakes_europe")
_NE_LAKES_NORTH_AMERICA = NESourceFile(area_type="physical", name="lakes_north_america")
_NE_RIVERS_EUROPE = NESourceFile(area_type="physical", name="rivers_europe")
_NE_RIVERS_NORTH_AMERICA = NESourceFile(area_type="physical", name="rivers_north_america")


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


class NEPlaceType(EnumWithValueLookup):
    airport = "Airport"
    alkaline_lake = "Alkaline Lake"
    lake = "Lake"
    lake_centerline = "Lake Centerline"
    national_park_service = "National Park Service"
    port = "Port"
    reservoir = "Reservoir"
    river = "River"

    @classmethod
    def from_feature_cla(cls, feature_cla: str) -> "NEPlaceType | None":
        if feature_cla.startswith("Intermittent") or feature_cla.endswith("Intermittent"):
            feature_cla = feature_cla.replace("Intermittent", "").strip()
        elif feature_cla.endswith("(Intermittent)"):
            feature_cla = feature_cla.replace("(Intermittent)", "").strip()
        return cls.from_value(feature_cla)

    def to_geoplace_type(self) -> GeoPlaceType:
        _place_type_map = {
            NEPlaceType.airport: GeoPlaceType.airport,
            NEPlaceType.alkaline_lake: GeoPlaceType.lake,
            NEPlaceType.lake: GeoPlaceType.lake,
            NEPlaceType.lake_centerline: GeoPlaceType.lake,
            NEPlaceType.reservoir: GeoPlaceType.lake,
            NEPlaceType.national_park_service: GeoPlaceType.national_park,
            NEPlaceType.port: GeoPlaceType.port,
            NEPlaceType.river: GeoPlaceType.river,
        }

        if self in _place_type_map:
            return _place_type_map[self]
        raise NotImplementedError(f"Missing mapping from NEPlaceType to GeoPlaceType for {self}")


class NEPlaceProperties(BaseModel):
    model_config = ConfigDict(strict=True, extra="allow", frozen=True)

    # TODO add the additional name fields I found

    name: str | None = None
    name_abb: str | None = None
    name_alt: str | None = None

    featurecla: str

    @field_validator("featurecla")
    @classmethod
    def validate_featurecla(cls, v: str) -> str:
        place_type = NEPlaceType.from_feature_cla(v)
        if place_type is None:
            raise ValueError(f"Unknown feature class: {v}")
        return v

    @cached_property
    def place_type(self) -> NEPlaceType:
        # This will always be valid because of the validator above
        place_type = NEPlaceType.from_feature_cla(self.featurecla)
        if place_type is None:
            raise ValueError(f"Unknown feature class: {self.featurecla}")
        return place_type

    @property
    def feature_name(self) -> str | None:
        return self.name or self.name_abb or self.name_alt


class NEFeature(Feature[NEPlaceProperties]):
    id: str = Field(
        description=(
            "Unique identifier for a natural earth feature. "
            "Note that these don't natively have them so we have to generate them."
        )
    )


def _get_ne_features_from_source(
    source_idx: int, source: NESourceFile
) -> Generator[NEFeature, None, None]:
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
        yield NEFeature.model_validate(feature)


def _ne_feature_to_geoplace(source: NESourceFile, feature: NEFeature) -> GeoPlace:
    props = feature.properties
    name = props.feature_name

    if name is None:
        raise Exception(f"Unexpected feature without name {feature}")

    return GeoPlace(
        id=feature.id,
        name=name,
        type=props.place_type.to_geoplace_type(),
        geom=feature.geometry,
        properties=props.model_dump(mode="json"),
        source=GeoPlaceSource(
            source_type=GeoPlaceSourceType.ne,
            source_path=source.url,
        ),
        # TODO finish this
        # hierarchies=props.hierarchies,
        # alternate_names=props.get_alternate_names(),
        # area_sq_km=props.area_square_m / 1000.0 if props.area_square_m else None,
        # population=props.population,
    )


def _get_all_ne_geoplaces() -> Generator[GeoPlace, None, None]:
    for source_idx, source in enumerate(_NE_SOURCE_FILES):
        for feature in _get_ne_features_from_source(source_idx, source):
            if (
                feature.properties.feature_name is not None
                and feature.properties.place_type != NEPlaceType.lake_centerline
            ):
                yield _ne_feature_to_geoplace(source, feature)


## Code for manual testing
# ruff: noqa: ERA001,T201,E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

all_places = list(_get_all_ne_geoplaces())
