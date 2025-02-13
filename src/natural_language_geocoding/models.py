from abc import ABC, abstractmethod
from functools import cached_property
from typing import Literal, Self

from e84_geoai_common.geometry import (
    BoundingBox,
    add_buffer,
    between,
    simplify_geometry,
)
from e84_geoai_common.util import singleline
from pydantic import BaseModel, ConfigDict, Field, RootModel
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.natural_earth import coastline_of
from natural_language_geocoding.place_lookup import PlaceLookup


class SpatialNodeType(BaseModel, ABC):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    @abstractmethod
    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry | None: ...


class NamedPlace(SpatialNodeType):
    """Represents the name of a place somewhere in the world."""

    node_type: Literal["NamedPlace"] = "NamedPlace"
    name: str

    subportion: Literal["western half", "northern half", "southern half", "eastern half"] | None = (
        Field(
            default=None,
            description=singleline(
                """
                An optional field to indicate that a subportion of the NamedPlace is referenced
                suchas "Western Brazil" would refer to the west half of Brazil. Note this is NOT
                used in cases where a cardinal direction is part of the place name like
                "South Africa"
            """
            ),
        )
    )

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry | None:
        geometry = place_lookup.search(self.name)
        if geometry is None:
            # FUTURE change this into a specific kind of exception that we can show the user.
            raise Exception(f"Unable to find area with name [{self.name}]")
        return simplify_geometry(geometry)


class CoastOf(SpatialNodeType):
    """Represents the coastline of an area."""

    node_type: Literal["CoastOf"] = "CoastOf"
    child_node: "AnySpatialNodeType"

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry | None:
        child_bounds = self.child_node.to_geometry(place_lookup)
        if child_bounds is None:
            return None
        return coastline_of(child_bounds)


class Buffer(SpatialNodeType):
    """Represents a spatial buffer outside the bounds of an existing node."""

    node_type: Literal["Buffer"] = "Buffer"
    child_node: "AnySpatialNodeType"
    distance: float
    distance_unit: Literal["kilometers", "meters", "miles"]

    @cached_property
    def distance_km(self) -> float:
        match self.distance_unit:
            case "kilometers":
                return self.distance
            case "meters":
                return self.distance / 1000.0
            case "miles":
                return self.distance * 1.60934
        raise Exception(f"Unexpected distance unit {self.distance_unit}")

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry | None:
        child_bounds = self.child_node.to_geometry(place_lookup)
        if child_bounds is None:
            return None

        return add_buffer(child_bounds, self.distance_km)


class DirectionalConstraint(BaseModel):
    """Constrains a spatial area by a direction.

    Example: 'west of London' represents the entire world west of London
    """

    node_type: Literal["DirectionalConstraint"] = "DirectionalConstraint"
    child_node: "AnySpatialNodeType"
    direction: Literal["west", "north", "south", "east"]

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry | None:
        child_bounds = self.child_node.to_geometry(place_lookup)
        if child_bounds is None:
            return None
        match self.direction:
            case "west":
                return BoundingBox(
                    west=-180.0,
                    east=child_bounds.bounds[0],
                    north=90.0,
                    south=-90.0,
                )
            case "east":
                return BoundingBox(
                    west=child_bounds.bounds[2],
                    east=180.0,
                    north=90.0,
                    south=-90.0,
                )
            case "north":
                return BoundingBox(
                    west=-180.0,
                    east=180.0,
                    north=90.0,
                    south=child_bounds.bounds[3],
                )
            case "south":
                return BoundingBox(
                    west=-180.0,
                    east=180.0,
                    north=child_bounds.bounds[1],
                    south=-90.0,
                )


class Intersection(SpatialNodeType):
    """Represents the spatial intersection of two areas."""

    node_type: Literal["Intersection"] = "Intersection"
    child_nodes: "list[AnySpatialNodeType]"

    @classmethod
    def from_nodes(cls, *nodes: "AnySpatialNodeType") -> Self:
        return cls(child_nodes=list(nodes))

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry | None:
        result: BaseGeometry | None = None
        for node in self.child_nodes:
            node_geom = node.to_geometry(place_lookup)
            result = result.intersection(node_geom) if result else node_geom
        return result


class Union(SpatialNodeType):
    """Represents the spatial union of two areas."""

    node_type: Literal["Union"] = "Union"
    child_nodes: "list[AnySpatialNodeType]"

    @classmethod
    def from_nodes(cls, *nodes: "AnySpatialNodeType") -> Self:
        return cls(child_nodes=list(nodes))

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry | None:
        result: BaseGeometry | None = None
        for node in self.child_nodes:
            node_geom = node.to_geometry(place_lookup)
            result = result.union(node_geom) if result else node_geom
        return result


class Difference(SpatialNodeType):
    """Represents the spatial difference of two areas."""

    node_type: Literal["Difference"] = "Difference"
    child_node_1: "AnySpatialNodeType"
    child_node_2: "AnySpatialNodeType"

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry | None:
        b1 = self.child_node_1.to_geometry(place_lookup)
        b2 = self.child_node_2.to_geometry(place_lookup)
        if b1 is None or b2 is None:
            return None
        return b1.difference(b2)


# FUTURE preprocess the query to change the way Between is implemented. If it's inside of another
# area that can be used as the contained bounds.


class Between(SpatialNodeType):
    """Represents a spatial area between two other areas."""

    node_type: Literal["Between"] = "Between"
    child_node_1: "AnySpatialNodeType"
    child_node_2: "AnySpatialNodeType"

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry | None:
        b1 = self.child_node_1.to_geometry(place_lookup)
        b2 = self.child_node_2.to_geometry(place_lookup)
        if b1 is None or b2 is None:
            return None

        return between(b1, b2)


AnySpatialNodeType = (
    NamedPlace
    | Buffer
    | CoastOf
    | Intersection
    | Union
    | Difference
    | Between
    | DirectionalConstraint
)


class SpatialNode(RootModel[AnySpatialNodeType]):
    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry | None:
        return self.root.to_geometry(place_lookup)
