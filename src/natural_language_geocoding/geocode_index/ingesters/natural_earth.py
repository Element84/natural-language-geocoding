"""Natural Earth Data Ingester for Geocoding Index.

This module provides functionality to download, process, and index geographic features
from the Natural Earth dataset into a geocoding index. Natural Earth is a public domain
map dataset available at naturalearthdata.com that provides cultural, physical, and
raster data themes.

Each feature is assigned a unique ID, processed for geometry validation, and enriched
with hierarchical relationships before being indexed for geocoding operations.
"""

import json
import logging
from collections.abc import Generator, Sequence
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
from natural_language_geocoding.geocode_index.index import (
    GeocodeIndex,
)
from natural_language_geocoding.geocode_index.ingesters.hierarchy_finder import get_hierarchies
from natural_language_geocoding.geocode_index.ingesters.ingest_utils import (
    counting_generator,
    fix_geometry,
    process_ingest_items,
)

_GITHUB_RAW_ROOT = "https://raw.githubusercontent.com/martynafford/natural-earth-geojson"

logger = logging.getLogger(__name__)

_LOCAL_TEMP_DIR = Path("temp")


class _NESourceFile(BaseModel):
    """Represents a source file from the Natural Earth dataset.

    This class handles the configuration and downloading of GeoJSON files from the
    Natural Earth dataset hosted on GitHub. It provides properties for generating
    URLs, local file paths, and handles the download process.

    Attributes:
        resolution: The resolution of the dataset (10m, 50m, or 110m)
        area_type: The type of geographic data (cultural or physical)
        name: The specific dataset name (e.g., 'airports', 'lakes')
    """

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
_NE_RIVERS_LAKES_CENTERLINES = _NESourceFile(area_type="physical", name="rivers_lake_centerlines")
_NE_RIVERS_EUROPE = _NESourceFile(area_type="physical", name="rivers_europe")
_NE_RIVERS_NORTH_AMERICA = _NESourceFile(area_type="physical", name="rivers_north_america")
_NE_GEOGRAPHY_REGIONS = _NESourceFile(area_type="physical", name="geography_regions_polys")
_NE_GEOGRAPHY_MARINE = _NESourceFile(area_type="physical", name="geography_marine_polys")

# A list of the source files to index along with their index for id generation.
# The ids are included in the tuples so that they can be temporarily commented out without changing
# the ids that are generated.
_NE_SOURCE_FILES: list[tuple[_NESourceFile, int]] = [
    (_NE_AIRPORTS, 0),
    (_NE_PORTS, 1),
    (_NE_PARKS_AND_PROTECTED_LANDS_AREA, 2),
    (_NE_LAKES, 3),
    (_NE_LAKES_EUROPE, 4),
    (_NE_LAKES_NORTH_AMERICA, 5),
    (_NE_RIVERS_LAKES_CENTERLINES, 6),
    (_NE_RIVERS_EUROPE, 7),
    (_NE_RIVERS_NORTH_AMERICA, 8),
    (_NE_GEOGRAPHY_REGIONS, 9),
    (_NE_GEOGRAPHY_MARINE, 10),
]


# FUTURE move this into a common place
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
    """Identifies the type of place from Natural Earth."""

    airport = "Airport"
    alkaline_lake = "Alkaline Lake"
    lake = "Lake"
    lake_centerline = "Lake Centerline"
    national_park_service = "National Park Service"
    port = "Port"
    reservoir = "Reservoir"
    river = "river"

    basin = "Basin"
    coast = "Coast"
    continent = "Continent"
    delta = "Delta"
    depression = "Depression"
    desert = "Desert"
    # From Null Island and skippable
    dragons_be_here = "Dragons-be-here"
    foothills = "Foothills"
    geoarea = "Geoarea"
    gorge = "Gorge"
    island = "Island"
    island_group = "Island group"
    isthmus = "Isthmus"
    lowland = "Lowland"
    pen_cape = "Pen/cape"
    peninsula = "Peninsula"
    plain = "Plain"
    plateau = "Plateau"
    range_mtn = "Range/mtn"
    tundra = "Tundra"
    valley = "Valley"
    wetlands = "Wetlands"

    bay = "bay"
    channel = "channel"
    fjord = "fjord"
    # Used to mean a generic water area (They're all internal water with no name.)
    generic = "generic"
    gulf = "gulf"
    inlet = "inlet"
    lagoon = "lagoon"
    ocean = "ocean"
    reef = "reef"
    sea = "sea"
    sound = "sound"
    strait = "strait"

    @classmethod
    def from_feature_cla(cls, feature_cla: str) -> "_NEPlaceType | None":
        if feature_cla.startswith("Intermittent") or feature_cla.endswith("Intermittent"):
            feature_cla = feature_cla.replace("Intermittent", "").strip()
        elif feature_cla.endswith("(Intermittent)"):
            feature_cla = feature_cla.replace("(Intermittent)", "").strip()
        if feature_cla == "River":
            # River and river both appear but we map them to a single type
            feature_cla = "river"
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
            _NEPlaceType.basin: GeoPlaceType.basin,
            _NEPlaceType.coast: GeoPlaceType.coast,
            _NEPlaceType.delta: GeoPlaceType.delta,
            _NEPlaceType.depression: GeoPlaceType.depression,
            _NEPlaceType.desert: GeoPlaceType.desert,
            _NEPlaceType.foothills: GeoPlaceType.foothills,
            _NEPlaceType.geoarea: GeoPlaceType.geoarea,
            _NEPlaceType.gorge: GeoPlaceType.gorge,
            _NEPlaceType.island: GeoPlaceType.island,
            _NEPlaceType.island_group: GeoPlaceType.island_group,
            _NEPlaceType.isthmus: GeoPlaceType.isthmus,
            _NEPlaceType.lowland: GeoPlaceType.lowland,
            _NEPlaceType.pen_cape: GeoPlaceType.peninsula,
            _NEPlaceType.peninsula: GeoPlaceType.peninsula,
            _NEPlaceType.plain: GeoPlaceType.plain,
            _NEPlaceType.plateau: GeoPlaceType.plateau,
            _NEPlaceType.range_mtn: GeoPlaceType.range_mtn,
            _NEPlaceType.tundra: GeoPlaceType.tundra,
            _NEPlaceType.valley: GeoPlaceType.valley,
            _NEPlaceType.wetlands: GeoPlaceType.wetlands,
            _NEPlaceType.bay: GeoPlaceType.bay,
            _NEPlaceType.channel: GeoPlaceType.channel,
            _NEPlaceType.fjord: GeoPlaceType.fjord,
            _NEPlaceType.gulf: GeoPlaceType.gulf,
            _NEPlaceType.inlet: GeoPlaceType.inlet,
            _NEPlaceType.lagoon: GeoPlaceType.lagoon,
            _NEPlaceType.reef: GeoPlaceType.reef,
            _NEPlaceType.sea: GeoPlaceType.sea,
            _NEPlaceType.sound: GeoPlaceType.sound,
            _NEPlaceType.strait: GeoPlaceType.strait,
        }

        if self in _place_type_map:
            return _place_type_map[self]
        raise NotImplementedError(f"Missing mapping from NEPlaceType to GeoPlaceType for {self}")


_SKIPPABLE_PLACE_TYPES = {
    # Dragons aren't real
    _NEPlaceType.dragons_be_here,
    # We only want lake polygons.
    _NEPlaceType.lake_centerline,
    # We have these from WOF already
    _NEPlaceType.continent,
    # We have these from WOF already
    _NEPlaceType.ocean,
    # Ignore these as they are all without a name and internal waters for each country
    _NEPlaceType.generic,
}


class _NEPlaceProperties(BaseModel):
    """Properties of a Natural Earth place feature."""

    model_config = ConfigDict(strict=True, extra="allow", frozen=True)

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
    """A Natural Earth geographic feature with properties and geometry."""

    id: str = Field(
        description=(
            "Unique identifier for a natural earth feature. "
            "Note that these don't natively have them so we have to generate them."
        )
    )


def _get_ne_features_from_source(
    source_idx: int, source: _NESourceFile
) -> Generator[_NEFeature, None, None]:
    """Get Natural Earth features from a source file.

    Downloads the source file if necessary and yields parsed features with generated IDs.

    Args:
        source_idx: Index of the source file for ID generation
        source: The Natural Earth source file to process

    Yields:
        _NEFeature: Parsed Natural Earth features with generated IDs
    """
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
        if feature.get("geometry") is not None:
            try:
                yield _NEFeature.model_validate(feature)
            except Exception as e:
                raise Exception(
                    f"Unable to parse feature [{feature.get('properties', {}).get('name', 'No name')}]"
                    f" from source {source}"
                ) from e


def _ne_feature_to_geoplace(
    index: GeocodeIndex, source: _NESourceFile, feature: _NEFeature
) -> GeoPlace:
    """Convert a Natural Earth feature to a GeoPlace for indexing.

    Args:
        index: The geocode index for hierarchy lookup
        source: The source file the feature came from
        feature: The Natural Earth feature to convert

    Returns:
        GeoPlace: A GeoPlace ready for indexing

    Raises:
        Exception: If the feature has no name
    """
    props = feature.properties
    name = props.feature_name

    if name is None:
        raise Exception(f"Unexpected feature without name {feature}")
    place_type = props.place_type.to_geoplace_type()
    fixed_geom = fix_geometry(feature.id, feature.geometry)
    hierarchies = get_hierarchies(index, fixed_geom)

    return GeoPlace(
        id=feature.id,
        place_name=name,
        type=place_type,
        geom=fixed_geom,
        properties=props.model_dump(mode="json"),
        source=GeoPlaceSource(
            source_type=GeoPlaceSourceType.ne,
            source_path=source.url,
        ),
        alternate_names=props.get_alternate_names(),
        hierarchies=list(hierarchies),
    )


def _get_all_ne_features() -> Generator[tuple[_NESourceFile, _NEFeature], None, None]:
    """Get all valid Natural Earth features from all configured source files.

    Filters out features without names and features with skippable place types.

    Yields:
        tuple[_NESourceFile, _NEFeature]: Source file and feature pairs
    """
    for source, source_idx in _NE_SOURCE_FILES:
        for feature in _get_ne_features_from_source(source_idx, source):
            if (
                feature.properties.feature_name is not None
                and feature.properties.place_type not in _SKIPPABLE_PLACE_TYPES
            ):
                yield (source, feature)


def _bulk_index_features(
    index: GeocodeIndex, source_features: Sequence[tuple[_NESourceFile, _NEFeature]]
) -> None:
    """Bulk index a batch of Natural Earth features into the geocode index.

    Args:
        index: The geocode index to add features to
        source_features: Sequence of (source, feature) pairs to index
    """
    places: list[GeoPlace] = [
        _ne_feature_to_geoplace(index, source, feature) for source, feature in source_features
    ]
    index.bulk_index(places)


def process_features() -> None:
    """Process and index all Natural Earth features into the geocode index.

    Downloads source files as needed, processes features, and bulk indexes them
    with progress logging.
    """
    logger.info("Starting to ingest natural earth features")
    process_ingest_items(
        counting_generator(_get_all_ne_features(), logger=logger), _bulk_index_features
    )

    logging.getLogger("opensearch").setLevel(logging.WARNING)
    logging.getLogger("natural_language_geocoding.geocode_index.index.GeocodeIndex").setLevel(
        logging.WARNING
    )

    process_features()


## Code for manual testing
# ruff: noqa: ERA001, E501

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
# )

# source_feature_pairs = list(_get_all_ne_features())


# def not_a_point(feature: _NEFeature) -> bool:
#     return not isinstance(feature.geometry, Point)


# by_name_type = group_by(
#     [f for _s, f in source_feature_pairs if not_a_point(f)],
#     key_fn=lambda f: (f.properties.name, f.properties.place_type),
# )


# by_name_type_multi = {
#     (name, place_type): features
#     for (name, place_type), features in by_name_type.items()
#     if len(features) > 1 and name is not None
# }


# for name, place_type in sorted(by_name_type_multi.keys()):
#     print(f"{name} - {place_type.value}")

# len(by_name_type_multi.keys())


# [f.properties for f in by_name_type_multi[("Reindeer Lake", _NEPlaceType.lake)]]


# display_geometry([f.geometry for f in by_name_type_multi[("Reindeer Lake", _NEPlaceType.lake)]])


# len(source_feature_pairs)

# len(source_feature_pairs)

# index = GeocodeIndex()

# subset_sfp = [
#     (source, feature)
#     for source, feature in source_feature_pairs
#     if feature.properties.name == "Mississippi"
# ]

# len(subset_sfp)

# display_geometry([subset_sfp[0][1].geometry])
# display_geometry([subset_sfp[1][1].geometry])
# display_geometry([subset_sfp[2][1].geometry])

# source, feature = subset_sfp[0]

# place = _ne_feature_to_geoplace(index, source, feature)

# print_hierarchies_with_names(index, place.hierarchies)

# display_geometry([place.geom])
