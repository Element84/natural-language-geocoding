"""Pydantic models for GeoJSON features."""

from typing import Annotated, Any, Literal, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    SkipValidation,
    field_serializer,
    field_validator,
)
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.common.geometry import geometry_from_geojson_dict


class Feature[T](BaseModel):
    """Represents a GeoJSON Feature with a shapely geometry and typed properties."""

    model_config = ConfigDict(strict=True, frozen=True, arbitrary_types_allowed=True)

    type: Literal["Feature"] = "Feature"
    geometry: Annotated[BaseGeometry, SkipValidation]
    properties: T

    @field_validator("geometry", mode="before")
    @classmethod
    def _parse_shapely_geometry(cls, d: Any) -> BaseGeometry:  # noqa: ANN401
        if isinstance(d, dict):
            return geometry_from_geojson_dict(cast("dict[str, Any]", d))
        if isinstance(d, BaseGeometry):
            return d
        msg = "geometry must be a geojson feature dictionary or a shapely geometry."
        raise TypeError(msg)

    @field_serializer("geometry")
    def _shapely_geometry_to_json(self, g: BaseGeometry) -> dict[str, Any]:
        return g.__geo_interface__


class FeatureCollection[T](BaseModel):
    """Represents a GeoJSON FeatureCollection."""

    # Extra fields are allowed to be open like GeoJSON is.
    model_config = ConfigDict(strict=True, frozen=True)
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: list[Feature[T]]
