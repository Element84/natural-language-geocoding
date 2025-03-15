from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from shapely.geometry.base import BaseGeometry


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


class GeoPlaceSourceType(Enum):
    wof = "wof"


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

    borough_id: int | None = None
    continent_id: int | None = None
    country_id: int | None = None
    county_id: int | None = None
    dependency_id: int | None = None
    disputed_id: int | None = None
    empire_id: int | None = None
    localadmin_id: int | None = None
    locality_id: int | None = None
    macrocounty_id: int | None = None
    macrohood_id: int | None = None
    macroregion_id: int | None = None
    marinearea_id: int | None = None
    marketarea_id: int | None = None
    microhood_id: int | None = None
    neighbourhood_id: int | None = None
    ocean_id: int | None = None
    postalregion_id: int | None = None
    region_id: int | None = None


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
    source_id: int
    source: GeoPlaceSource
    alternate_names: list[str] = Field(default_factory=list)
    hierarchies: list[Hierarchy] = Field(default_factory=list)
    properties: dict[str, Any]
