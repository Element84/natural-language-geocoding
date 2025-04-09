"""Defines types that represent places on the earth."""

from enum import Enum
from typing import Annotated, Any, cast

from e84_geoai_common.geometry import geometry_from_geojson_dict
from pydantic import BaseModel, ConfigDict, Field, SkipValidation, field_serializer, field_validator
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

    basin = "basin"
    coast = "coast"
    delta = "delta"
    depression = "depression"
    desert = "desert"
    foothills = "foothills"
    geoarea = "geoarea"
    gorge = "gorge"
    island = "island"
    island_group = "island_group"
    isthmus = "isthmus"
    lowland = "lowland"
    peninsula = "peninsula"
    plain = "plain"
    plateau = "plateau"
    range_mtn = "range_mtn"
    tundra = "tundra"
    valley = "valley"
    wetlands = "wetlands"
    bay = "bay"
    channel = "channel"
    fjord = "fjord"
    generic = "generic"
    gulf = "gulf"
    inlet = "inlet"
    lagoon = "lagoon"
    reef = "reef"
    sea = "sea"
    sound = "sound"
    strait = "strait"


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
    # Composed areas that are composed of other areas
    comp = "comp"


class GeoPlaceSource(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    source_type: GeoPlaceSourceType
    source_path: str


class Hierarchy(BaseModel, frozen=True):
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

    def with_id(self, feature_id: str, place_type: GeoPlaceType) -> "Hierarchy":
        """Creates a new hierarchy with the specified id set."""
        model = self.model_dump()
        model[f"{place_type.value}_id"] = feature_id
        return Hierarchy.model_validate(model)


class GeoPlace(BaseModel):
    model_config = ConfigDict(
        strict=True, extra="forbid", frozen=True, arbitrary_types_allowed=True
    )

    id: str
    place_name: str
    type: GeoPlaceType
    geom: Annotated[BaseGeometry, SkipValidation]
    source: GeoPlaceSource
    alternate_names: list[str] = Field(default_factory=list)
    hierarchies: list[Hierarchy] = Field(default_factory=list)
    area_sq_km: float | None = None
    population: int | None = None
    properties: dict[str, Any]

    @field_validator("geom", mode="before")
    @classmethod
    def _parse_shapely_geometry(cls, d: Any) -> BaseGeometry:  # noqa: ANN401
        if isinstance(d, dict):
            return geometry_from_geojson_dict(cast("dict[str, Any]", d))
        if isinstance(d, BaseGeometry):
            return d
        msg = "geometry must be a geojson feature dictionary or a shapely geometry."
        raise TypeError(msg)

    @field_serializer("geom")
    def _shapely_geometry_to_json(self, g: BaseGeometry) -> dict[str, Any]:
        return g.__geo_interface__

    def self_as_hierarchies(self) -> list[Hierarchy]:
        """Returns a set of hierarchies representing this place in the hierarchy."""
        if len(self.hierarchies) > 0:
            return [h.with_id(self.id, self.type) for h in self.hierarchies]
        model = {f"{self.type.value}_id": self.id}
        return [Hierarchy.model_validate(model)]


def print_places_as_table(places: list[GeoPlace]) -> None:
    """Prints places as a table. Useful for debugging."""
    table_data: list[dict[str, Any]] = []
    for index, place in enumerate(places):
        place_dict = {
            "index": index,
            "id": place.id,
            "name": place.place_name,
            "type": place.type.value,
            "alternate_names": place.alternate_names,
            "hierarchies": [{k: v for k, v in h if v is not None} for h in place.hierarchies],
        }
        table_data.append(place_dict)

    # Print the table
    print(tabulate(table_data, headers="keys", tablefmt="grid"))  # noqa: T201


def print_hierarchies_as_table(hierarchies: list[Hierarchy]) -> None:
    """Prints hierarchies as a table. Useful for debugging."""
    table_data: list[dict[str, Any]] = [h.model_dump(exclude_none=True) for h in hierarchies]

    # Print the table
    print(tabulate(table_data, headers="keys", tablefmt="grid"))  # noqa: T201
