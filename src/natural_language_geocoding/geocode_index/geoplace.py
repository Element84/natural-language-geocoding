from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from shapely.geometry.base import BaseGeometry


class GeoPlaceType(Enum):
    """The set of different place types that are supported.

    Based on a subset of the Who's On First placetypes.
    """

    continent = "continent"
    ocean = "ocean"
    country = "country"
    empire = "empire"
    locality = "locality"
    dependency = "dependency"
    disputed = "disputed"
    region = "region"
    localadmin = "localadmin"
    borough = "borough"
    county = "county"
    macrocounty = "macrocounty"
    macrohood = "macrohood"
    macroregion = "macroregion"
    marinearea = "marinearea"
    marketarea = "marketarea"
    microhood = "microhood"
    neighbourhood = "neighbourhood"
    postalregion = "postalregion"


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

    continent_id: int | None
    empire_id: int | None = None
    country_id: int | None
    locality_id: int | None
    macroregion_id: int | None
    region_id: int | None


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
    hierarchy: Hierarchy | None
    properties: dict[str, Any]
