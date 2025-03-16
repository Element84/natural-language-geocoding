from abc import ABC, abstractmethod
from functools import cached_property
from typing import Literal, Self

from e84_geoai_common.geometry import (
    BoundingBox,
    add_buffer,
    between,
    simplify_geometry,
)
from pydantic import BaseModel, ConfigDict, Field, RootModel
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.geocode_index.geoplace import GeoPlaceType
from natural_language_geocoding.natural_earth import coastline_of
from natural_language_geocoding.place_lookup import PlaceLookup

# TODO Issue for the ability to specify a portion of an area like the northern part or southern part
# This was originally here but the implementation ignored it.


class SpatialNodeType(BaseModel, ABC):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    @abstractmethod
    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry | None: ...


class NamedPlace(SpatialNodeType):
    """Represents a place on the earth locatable in a geocoding database."""

    node_type: Literal["NamedPlace"] = "NamedPlace"
    name: str = Field(description="The name to use to find the location")
    type: GeoPlaceType | None = Field(
        default=None, description="Limits the search to a specific type of location"
    )

    in_continent: str | None = Field(
        default=None,
        description="Indicates to search within a specific continent.",
    )
    in_country: str | None = Field(
        default=None,
        description="Indicates to search within a specific country.",
    )
    in_region: str | None = Field(
        default=None,
        description="Indicates to search within a specific region such as a specific US state.",
    )

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry | None:
        geometry = place_lookup.search(
            name=self.name,
            place_type=self.type,
            in_continent=self.in_continent,
            in_country=self.in_country,
            in_region=self.in_region,
        )
        if geometry is None:
            # FUTURE change this into a specific kind of exception that we can show the user.
            raise Exception(f"Unable to find area with name [{self.name}]")
        return simplify_geometry(geometry)


class CoastOf(SpatialNodeType):
    """Represents the coastline of an area."""

    node_type: Literal["CoastOf"] = "CoastOf"
    child_node: "AnySpatialNodeType"

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry | None:
        if child_bounds := self.child_node.to_geometry(place_lookup):
            return coastline_of(child_bounds)
        return None


# The number of kilometers of buffer added to each shape to ensure that the areas that are very
# close to intersection do intersect when computing border collisions
_BORDER_BUFFER_SIZE = 3.5


def border_between(g1: BaseGeometry, g2: BaseGeometry) -> BaseGeometry | None:
    """Computes the border between two geometries."""
    c1 = add_buffer(g1, _BORDER_BUFFER_SIZE)
    c2 = add_buffer(g2, _BORDER_BUFFER_SIZE)

    if c1.intersects(c2):
        return c1.intersection(c2)
    return None


class BorderBetween(SpatialNodeType):
    """Represents the adjoining border of two areas that are adjacent to each other.

    Example: the border between North Dakota and South Dakota would be a very short, wide polygon
    that covers the area where North and South Dakota connect.
    """

    node_type: Literal["BorderBetween"] = "BorderBetween"
    child_node_1: "AnySpatialNodeType"
    child_node_2: "AnySpatialNodeType"

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry | None:
        child1_bounds = self.child_node_1.to_geometry(place_lookup)
        child2_bounds = self.child_node_2.to_geometry(place_lookup)
        if child1_bounds is None or child2_bounds is None:
            return None

        return border_between(child1_bounds, child2_bounds)


class BorderOf(SpatialNodeType):
    """Represents the border of an area as one or more LineStrings."""

    node_type: Literal["BorderOf"] = "BorderOf"
    child_node: "AnySpatialNodeType"

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry | None:
        if child_bounds := self.child_node.to_geometry(place_lookup):
            boundary = child_bounds.boundary

            if boundary.is_empty:
                return None
            return boundary
        return None


class Buffer(SpatialNodeType):
    """Represents a spatial buffer outside the bounds of an existing node."""

    node_type: Literal["Buffer"] = "Buffer"
    child_node: "AnySpatialNodeType"
    distance: float
    distance_unit: Literal["kilometers", "meters", "miles", "nautical miles"]

    @cached_property
    def distance_km(self) -> float:
        match self.distance_unit:
            case "kilometers":
                return self.distance
            case "meters":
                return self.distance / 1000.0
            case "miles":
                return self.distance * 1.60934
            case "nautical miles":
                return self.distance * 1.852

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry | None:
        if child_bounds := self.child_node.to_geometry(place_lookup):
            return add_buffer(child_bounds, self.distance_km)
        return None


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
    | BorderBetween
    | BorderOf
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
