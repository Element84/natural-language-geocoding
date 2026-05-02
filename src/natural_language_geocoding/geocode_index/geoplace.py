"""Defines types that represent geospatial places.

Geo-prefixed types are generic (body-agnostic).
Earth-prefixed types are Earth-specific.
"""

from enum import Enum
from typing import Annotated, Any, cast

from e84_geoai_common.geometry import geometry_from_geojson_dict
from pydantic import BaseModel, ConfigDict, Field, SkipValidation, field_serializer, field_validator
from shapely.geometry.base import BaseGeometry

# =============================================================================
# Generic (body-agnostic) types
# =============================================================================


class GeoPlaceSource(BaseModel):
    """Generic source identifier for any geospatial place."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    source_type: str
    source_path: str


class GeoPlace(BaseModel):
    """Base model for any geospatial place, body-agnostic.

    This is the common interface shared by Earth places, Lunar places, and any
    future celestial body. Body-specific models extend this with additional fields.
    """

    model_config = ConfigDict(
        strict=True, extra="forbid", frozen=True, arbitrary_types_allowed=True
    )

    id: str
    place_name: str
    type: str
    geom: Annotated[BaseGeometry, SkipValidation]
    source: GeoPlaceSource
    alternate_names: list[str] = Field(default_factory=list)
    area_sq_km: float | None = None
    properties: dict[str, Any] = Field(default_factory=dict)

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


# =============================================================================
# Earth-specific types
# =============================================================================


class EarthPlaceType(Enum):
    """The set of different Earth place types that are supported.

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
DEFAULT_PLACE_TYPE_SORT_ORDER: list[EarthPlaceType | str] = [
    EarthPlaceType.continent,
    EarthPlaceType.country,
    EarthPlaceType.empire,
    EarthPlaceType.region,
    EarthPlaceType.marinearea,
    EarthPlaceType.ocean,
    EarthPlaceType.geoarea,
    EarthPlaceType.locality,
    EarthPlaceType.county,
    EarthPlaceType.postalregion,
]


class EarthPlaceSourceType(Enum):
    # Who's on First
    wof = "wof"
    # Natural Earth
    ne = "ne"
    # Composed areas that are composed of other areas
    comp = "comp"


# The sort order for search results by source type. If the source type is not in this list then it
# should appear after any of these
DEFAULT_SOURCE_TYPE_SORT_ORDER: list[EarthPlaceSourceType | str] = [
    EarthPlaceSourceType.comp,
    EarthPlaceSourceType.ne,
    EarthPlaceSourceType.wof,
]


class EarthPlaceSource(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    source_type: EarthPlaceSourceType | str
    source_path: str

    @field_validator("source_type", mode="before")
    @classmethod
    def _parse_place_source_type(cls, v: Any) -> EarthPlaceSourceType | str:  # noqa: ANN401
        if isinstance(v, str):
            try:
                return EarthPlaceSourceType(v)
            except ValueError:
                return v
        if isinstance(v, EarthPlaceSourceType):
            return v
        msg = "source_type must be a string or GeoPlaceSourceType."
        raise TypeError(msg)

    @property
    def source_type_value(self) -> str:
        """Returns the source type as a string."""
        return self.source_type if isinstance(self.source_type, str) else self.source_type.value


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

    def get_by_place_type(self, place_type: EarthPlaceType) -> str | None:
        return getattr(self, f"{place_type.value}_id")

    def with_id(self, feature_id: str, place_type: EarthPlaceType | str) -> "Hierarchy":
        """Creates a new hierarchy with the specified id set."""
        model = self.model_dump()
        place_type_value = place_type if isinstance(place_type, str) else place_type.value
        model[f"{place_type_value}_id"] = feature_id
        return Hierarchy.model_validate(model)


class EarthPlace(GeoPlace):
    """An Earth-specific geospatial place with hierarchies and population."""

    id: str
    place_name: str
    type: EarthPlaceType | str
    geom: Annotated[BaseGeometry, SkipValidation]
    source: EarthPlaceSource
    alternate_names: list[str] = Field(default_factory=list)
    hierarchies: list[Hierarchy] = Field(default_factory=lambda: [])  # noqa: PIE807
    area_sq_km: float | None = None
    population: int | None = None
    properties: dict[str, Any] = Field(default_factory=dict)

    @field_validator("type", mode="before")
    @classmethod
    def _parse_place_type(cls, v: Any) -> EarthPlaceType | str:  # noqa: ANN401
        if isinstance(v, str):
            try:
                return EarthPlaceType(v)
            except ValueError:
                return v
        if isinstance(v, EarthPlaceType):
            return v
        msg = "type must be a string or EarthPlaceType."
        raise TypeError(msg)

    @property
    def type_value(self) -> str:
        """Returns the place type as a string."""
        return self.type if isinstance(self.type, str) else self.type.value

    def display_geometry(self) -> Any:  # noqa: ANN401
        """Displays geometry in a jupyter like environment for debugging."""
        from e84_geoai_common.debugging import display_geometry  # noqa: PLC0415

        return display_geometry([self.geom])

    def self_as_hierarchies(self) -> list[Hierarchy]:
        """Returns a set of hierarchies representing this place in the hierarchy."""
        if len(self.hierarchies) > 0:
            return [h.with_id(self.id, self.type) for h in self.hierarchies]
        model = {f"{self.type_value}_id": self.id}
        return [Hierarchy.model_validate(model)]
