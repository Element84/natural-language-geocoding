"""TODO document this module."""

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from shapely.geometry.base import BaseGeometry
from tabulate import tabulate


class GeoPlaceType(Enum):
    """The set of different place types that are supported.

    Based on a subset of the Who's On First placetypes.
    """

    borough = "borough"
    continent = "continent"
    country = "country"
    county = "county"
    dependency = "dependency"
    disputed = "disputed"
    empire = "empire"
    localadmin = "localadmin"
    locality = "locality"
    macrocounty = "macrocounty"
    macrohood = "macrohood"
    macroregion = "macroregion"
    marinearea = "marinearea"
    marketarea = "marketarea"
    microhood = "microhood"
    neighbourhood = "neighbourhood"
    ocean = "ocean"
    postalregion = "postalregion"
    region = "region"

    port = "port"
    airport = "airport"
    lake = "lake"
    national_park = "national_park"
    river = "river"


# The sort order for search results by place type. If the place type is not in this list then it
# should appear after any of these
PLACE_TYPE_SORT_ORDER = [
    GeoPlaceType.continent,
    GeoPlaceType.country,
    GeoPlaceType.empire,
    GeoPlaceType.region,
    GeoPlaceType.locality,
    GeoPlaceType.county,
    GeoPlaceType.marinearea,
    GeoPlaceType.ocean,
    GeoPlaceType.postalregion,
]


class GeoPlaceSourceType(Enum):
    # Who's on First
    wof = "wof"
    # Natural Earth
    ne = "ne"


class GeoPlaceSource(BaseModel):
    model_config = ConfigDict(
        strict=True,
        extra="forbid",
        frozen=True,
        json_encoders={
            GeoPlaceSourceType: lambda x: x.value,
        },
    )
    source_type: GeoPlaceSourceType
    source_path: str


class Hierarchy(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    borough_id: str | None = None
    continent_id: str | None = None
    country_id: str | None = None
    county_id: str | None = None
    dependency_id: str | None = None
    disputed_id: str | None = None
    empire_id: str | None = None
    localadmin_id: str | None = None
    locality_id: str | None = None
    macrocounty_id: str | None = None
    macrohood_id: str | None = None
    macroregion_id: str | None = None
    marinearea_id: str | None = None
    marketarea_id: str | None = None
    microhood_id: str | None = None
    neighbourhood_id: str | None = None
    ocean_id: str | None = None
    postalregion_id: str | None = None
    region_id: str | None = None

    def get_by_place_type(self, place_type: GeoPlaceType) -> str | None:
        return getattr(self, f"{place_type.value}_id")


class GeoPlace(BaseModel):
    model_config = ConfigDict(
        strict=True,
        extra="forbid",
        frozen=True,
        arbitrary_types_allowed=True,
        json_encoders={
            GeoPlaceType: lambda x: x.value,
            BaseGeometry: lambda x: x.__geo_interface__,
        },
    )

    id: str
    name: str
    type: GeoPlaceType
    geom: BaseGeometry
    source: GeoPlaceSource
    alternate_names: list[str] = Field(default_factory=list)
    hierarchies: list[Hierarchy] = Field(default_factory=list)
    area_sq_km: float | None = None
    population: int | None = None
    properties: dict[str, Any]


def print_places_as_table(places: list[GeoPlace]) -> None:
    """Prints places as a table. Useful for debugging."""
    table_data: list[dict[str, Any]] = []
    for index, place in enumerate(places):
        place_dict = {
            "index": index,
            "id": place.id,
            "name": place.name,
            "type": place.type.value,
            "alternate_names": place.alternate_names,
            "hierarchies": [{k: v for k, v in h if v is not None} for h in place.hierarchies],
        }
        table_data.append(place_dict)

    # Print the table
    print(tabulate(table_data, headers="keys", tablefmt="grid"))  # noqa: T201
