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
_NE_RIVERS_LAKES_CENTERLINES = _NESourceFile(area_type="physical", name="rivers_lake_centerlines")
_NE_RIVERS_EUROPE = _NESourceFile(area_type="physical", name="rivers_europe")
_NE_RIVERS_NORTH_AMERICA = _NESourceFile(area_type="physical", name="rivers_north_america")
_NE_GEOGRAPHY_REGIONS = _NESourceFile(area_type="physical", name="geography_regions_polys")
_NE_GEOGRAPHY_MARINE = _NESourceFile(area_type="physical", name="geography_marine_polys")


_NE_SOURCE_FILES = [
    # TODO temporarily commenting
    # _NE_AIRPORTS,
    # _NE_PORTS,
    # _NE_PARKS_AND_PROTECTED_LANDS_AREA,
    # _NE_LAKES,
    # _NE_LAKES_EUROPE,
    # _NE_LAKES_NORTH_AMERICA,
    # _NE_RIVERS_LAKES_CENTERLINES,
    # _NE_RIVERS_EUROPE,
    # _NE_RIVERS_NORTH_AMERICA,
    _NE_GEOGRAPHY_REGIONS,
    _NE_GEOGRAPHY_MARINE,
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
    river = "river"

    basin = "Basin"
    coast = "Coast"
    # We'll skip this since we have it from WOF
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
    # Ignore these as they are all without a name and internal waters for each country
    generic = "generic"
    gulf = "gulf"
    inlet = "inlet"
    lagoon = "lagoon"
    # We'll skip this since we have it from WOF
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
    """TODO docs."""
    props = feature.properties
    name = props.feature_name

    if name is None:
        raise Exception(f"Unexpected feature without name {feature}")
    place_type = props.place_type.to_geoplace_type()
    fixed_geom = fix_geometry(feature.id, feature.geometry)
    hierarchies = get_hierarchies(index, name, place_type, fixed_geom)

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
    places: list[GeoPlace] = [
        _ne_feature_to_geoplace(index, source, feature) for source, feature in source_features
    ]
    index.bulk_index(places)


def process_features() -> None:
    """TODO docs."""
    logger.info("Starting to ingest natural earth features")
    process_ingest_items(
        counting_generator(_get_all_ne_features(), logger=logger), _bulk_index_features
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logging.getLogger("opensearch").setLevel(logging.WARNING)
    logging.getLogger("natural_language_geocoding.geocode_index.index.GeocodeIndex").setLevel(
        logging.WARNING
    )

    process_features()

## Code for manual testing
# ruff: noqa: ERA001,T201,E402,E501


# def print_hierarchies_with_names(index: GeocodeIndex, hierarchies: list[Hierarchy]) -> None:
#     """Prints hierarchies as a table. Useful for debugging."""
#     places = index.get_by_ids(
#         [place_id for h in hierarchies for place_id in h.model_dump(exclude_none=True).values()]
#     )
#     id_to_name = {p.id: p.place_name for p in places}

#     table_data: list[dict[str, Any]] = [
#         {field: id_to_name[place_id] for field, place_id in h.model_dump(exclude_none=True).items()}
#         for h in hierarchies
#     ]

#     # Print the table
#     print(tabulate(table_data, headers="keys", tablefmt="grid"))


# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
# )

# source_feature_pairs = list(_get_all_ne_features())

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
