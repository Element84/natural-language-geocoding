from enum import Enum
from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field
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

    borough_id: int | None = Field(
        validation_alias=AliasChoices("borough_id", "borough"), default=None
    )
    continent_id: int | None = Field(
        validation_alias=AliasChoices("continent_id", "continent"), default=None
    )
    country_id: int | None = Field(
        validation_alias=AliasChoices("country_id", "country"), default=None
    )
    county_id: int | None = Field(
        validation_alias=AliasChoices("county_id", "county"), default=None
    )
    dependency_id: int | None = Field(
        validation_alias=AliasChoices("dependency_id", "dependency"), default=None
    )
    disputed_id: int | None = Field(
        validation_alias=AliasChoices("disputed_id", "disputed"), default=None
    )
    empire_id: int | None = Field(
        validation_alias=AliasChoices("empire_id", "empire"), default=None
    )
    localadmin_id: int | None = Field(
        validation_alias=AliasChoices("localadmin_id", "localadmin"), default=None
    )
    locality_id: int | None = Field(
        validation_alias=AliasChoices("locality_id", "locality"), default=None
    )
    macrocounty_id: int | None = Field(
        validation_alias=AliasChoices("macrocounty_id", "macrocounty"), default=None
    )
    macrohood_id: int | None = Field(
        validation_alias=AliasChoices("macrohood_id", "macrohood"), default=None
    )
    macroregion_id: int | None = Field(
        validation_alias=AliasChoices("macroregion_id", "macroregion"), default=None
    )
    marinearea_id: int | None = Field(
        validation_alias=AliasChoices("marinearea_id", "marinearea"), default=None
    )
    marketarea_id: int | None = Field(
        validation_alias=AliasChoices("marketarea_id", "marketarea"), default=None
    )
    microhood_id: int | None = Field(
        validation_alias=AliasChoices("microhood_id", "microhood"), default=None
    )
    neighbourhood_id: int | None = Field(
        validation_alias=AliasChoices("neighbourhood_id", "neighbourhood"), default=None
    )
    ocean_id: int | None = Field(validation_alias=AliasChoices("ocean_id", "ocean"), default=None)
    postalregion_id: int | None = Field(
        validation_alias=AliasChoices("postalregion_id", "postalregion"), default=None
    )
    region_id: int | None = Field(
        validation_alias=AliasChoices("region_id", "region"), default=None
    )


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
