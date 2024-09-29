from abc import ABC, abstractmethod
from functools import cached_property

from pydantic import BaseModel, ConfigDict, Field, RootModel
from typing import Literal


from e84_geoai_common.util import singleline
from natural_language_geocoding.nominatim import nominatim_search
from e84_geoai_common.geometry import (
    BoundingBox,
    add_buffer,
    between,
    simplify_geometry,
)
from natural_language_geocoding.natural_earth import coastline_of
from shapely.geometry.base import BaseGeometry


class SpatialNodeType(BaseModel, ABC):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    @abstractmethod
    def to_geometry(self) -> BaseGeometry | None: ...


class NamedEntity(SpatialNodeType):
    """Represents the name of a place somewhere in the world"""

    node_type: Literal["NamedEntity"] = "NamedEntity"
    name: str

    subportion: (
        Literal["western half", "northern half", "southern half", "eastern half"] | None
    ) = Field(
        default=None,
        description=singleline(
            """
                An optional field to indicate that a subportion of the NamedEntity is referenced
                suchas "Western Brazil" would refer to the west half of Brazil. Note this is NOT
                used in cases where a cardinal direction is part of the place name like "South Africa"
            """
        ),
    )

    def to_geometry(self) -> BaseGeometry | None:
        geometry = nominatim_search(self.name)
        if geometry is None:
            # FUTURE change this into a specific kind of exception that we can show the user.
            raise Exception(f"Unable to find area with name [{self.name}]")
        return simplify_geometry(geometry)


class CoastOf(SpatialNodeType):
    """Represents the coastline of an area."""

    node_type: Literal["CoastOf"] = "CoastOf"
    child_node: "SpatialNode"

    def to_geometry(self) -> BaseGeometry | None:
        child_bounds = self.child_node.to_geometry()
        if child_bounds is None:
            return None
        return coastline_of(child_bounds)


class Buffer(SpatialNodeType):
    """Represents a spatial buffer outside the bounds of an existing node."""

    node_type: Literal["Buffer"]
    child_node: "SpatialNode"
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

    def to_geometry(self) -> BaseGeometry | None:
        child_bounds = self.child_node.to_geometry()
        if child_bounds is None:
            return None

        return add_buffer(child_bounds, self.distance_km)


class DirectionalConstraint(BaseModel):
    """Constrains a spatial area such that it will be "west of", "north of", etc a particular spatial area."""

    node_type: Literal["DirectionalConstraint"]
    child_node: "SpatialNode"
    direction: Literal["west", "north", "south", "east"]

    def to_geometry(self) -> BaseGeometry | None:
        child_bounds = self.child_node.to_geometry()
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
    """Represents the spatial intersection of two areas"""

    node_type: Literal["Intersection"]
    child_node_1: "SpatialNode"
    child_node_2: "SpatialNode"

    def to_geometry(self) -> BaseGeometry | None:
        b1 = self.child_node_1.to_geometry()
        b2 = self.child_node_2.to_geometry()
        if b1 is None or b2 is None:
            return None
        return b1.intersection(b2)


class Union(SpatialNodeType):
    """Represents the spatial union of two areas"""

    node_type: Literal["Union"]
    child_node_1: "SpatialNode"
    child_node_2: "SpatialNode"

    def to_geometry(self) -> BaseGeometry | None:
        b1 = self.child_node_1.to_geometry()
        b2 = self.child_node_2.to_geometry()
        if b1 is None or b2 is None:
            return None
        return b1.union(b2)


class Difference(SpatialNodeType):
    """Represents the spatial difference of two areas"""

    node_type: Literal["Difference"]
    child_node_1: "SpatialNode"
    child_node_2: "SpatialNode"

    def to_geometry(self) -> BaseGeometry | None:
        b1 = self.child_node_1.to_geometry()
        b2 = self.child_node_2.to_geometry()
        if b1 is None or b2 is None:
            return None
        return b1.difference(b2)


# FUTURE preprocess the query to change the way between is implemented. If it's inside of another area
# that can be used as the contained bounds.


class Between(SpatialNodeType):
    """Represents a spatial area between two other areas."""

    node_type: Literal["Between"]
    child_node_1: "SpatialNode"
    child_node_2: "SpatialNode"

    def to_geometry(self) -> BaseGeometry | None:
        b1 = self.child_node_1.to_geometry()
        b2 = self.child_node_2.to_geometry()
        if b1 is None or b2 is None:
            return None

        return between(b1, b2)


AnySpatialNodeType = (
    NamedEntity
    | Buffer
    | CoastOf
    | Intersection
    | Union
    | Difference
    | Between
    | DirectionalConstraint
)


class SpatialNode(RootModel[AnySpatialNodeType]):

    def to_geometry(self) -> BaseGeometry | None:
        return self.root.to_geometry()
