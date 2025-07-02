from abc import ABC, abstractmethod
from typing import Any, Literal, Self

from e84_geoai_common.geometry import (
    BoundingBox,
    add_buffer,
    between,
    simplify_geometry,
)
from pydantic import BaseModel, ConfigDict, Field, RootModel, field_validator
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.errors import GeocodeError
from natural_language_geocoding.geocode_index.geoplace import GeoPlaceType
from natural_language_geocoding.natural_earth import coastline_of
from natural_language_geocoding.place_lookup import PlaceLookup, PlaceSearchRequest


def _to_kilometers(
    distance_unit: Literal["kilometers", "meters", "miles", "nautical miles"], distance: float
) -> float:
    match distance_unit:
        case "kilometers":
            return distance
        case "meters":
            return distance / 1000.0
        case "miles":
            return distance * 1.60934
        case "nautical miles":
            return distance * 1.852


class SpatialNodeType(BaseModel, ABC):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    @abstractmethod
    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry: ...


class NamedPlace(SpatialNodeType):
    """Represents a place on the earth locatable in a geocoding database."""

    node_type: Literal["NamedPlace"] = "NamedPlace"
    name: str = Field(
        description=(
            "The name to use to find the location. Correct the name the user specifies as needed "
            "to help ensure the correct area is found."
        )
    )
    type: GeoPlaceType | str | None = Field(
        default=None, description="Limits the search to a specific type of location"
    )

    in_continent: str | None = Field(
        default=None,
        description=(
            "Indicates to search within a specific continent."
            " Note that Oceania is a valid continent."
        ),
    )
    in_country: str | None = Field(
        default=None,
        description="Indicates to search within a specific country.",
    )
    in_region: str | None = Field(
        default=None,
        description=(
            "Indicates to search within a specific region such as a specific US state. "
            "Region names are not globally unique so in_country must be specified as well."
        ),
    )

    @field_validator("type", mode="before")
    @classmethod
    def _parse_place_type(cls, v: Any) -> GeoPlaceType | str | None:  # noqa: ANN401
        if v is None:
            return v
        if isinstance(v, str):
            try:
                return GeoPlaceType(v)
            except ValueError:
                return v
        if isinstance(v, GeoPlaceType):
            return v
        msg = "type must be None, a string, or GeoPlaceType."
        raise TypeError(msg)

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry:
        geometry = place_lookup.search(
            PlaceSearchRequest(
                name=self.name,
                place_type=self.type,
                in_continent=self.in_continent,
                in_country=self.in_country,
                in_region=self.in_region,
            )
        )
        # Simplify the geometry to enable faster additional processing of the geometry.
        # 18,500 points was found to be a number that worked even for areas with many
        # small polygons. A smaller number fails because countries with many small islands
        # can have thousands of separate polygons which are difficult to simplify.
        return simplify_geometry(geometry, 18_500)


class CoastOf(SpatialNodeType):
    """Represents the coastline of an area."""

    node_type: Literal["CoastOf"] = "CoastOf"
    child_node: "AnySpatialNodeType"

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry:
        child_bounds = self.child_node.to_geometry(place_lookup)
        coast = coastline_of(child_bounds)
        if coast is None:
            # FUTURE these errors would benefit from being more specific.
            raise GeocodeError("Could not find a coastline of the area.")
        return coast


class OffTheCoastOf(SpatialNodeType):
    """Represents the area off the coast of an area away from land."""

    # FUTURE add directional constraint in here like "off the west coast of the US"

    node_type: Literal["OffTheCoastOf"] = "OffTheCoastOf"
    child_node: "AnySpatialNodeType"
    distance: float
    distance_unit: Literal["kilometers", "meters", "miles", "nautical miles"]

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry:
        # Find the area mentioned
        child_bounds = self.child_node.to_geometry(place_lookup)
        # Get it's coastline
        coast = coastline_of(child_bounds)
        if coast is None:
            # FUTURE these errors would benefit from being more specific.
            raise GeocodeError("Could not find a coastline of the area.")
        # Add a buffer of the specified amount.
        buffered_coast = add_buffer(coast, _to_kilometers(self.distance_unit, self.distance))
        # Remove the original place itself that would be covered by the buffer.
        return buffered_coast.difference(child_bounds)


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

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry:
        child1_bounds = self.child_node_1.to_geometry(place_lookup)
        child2_bounds = self.child_node_2.to_geometry(place_lookup)

        intersection = border_between(child1_bounds, child2_bounds)
        if intersection is None:
            raise GeocodeError("No border found between the two areas")
        return intersection


class BorderOf(SpatialNodeType):
    """Represents the border of an area as one or more LineStrings."""

    node_type: Literal["BorderOf"] = "BorderOf"
    child_node: "AnySpatialNodeType"

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry:
        child_bounds = self.child_node.to_geometry(place_lookup)
        boundary = child_bounds.boundary

        if boundary.is_empty:
            raise GeocodeError("Could not find border of area")
        return boundary


class Buffer(SpatialNodeType):
    """Represents a spatial buffer outside the bounds of an existing node."""

    node_type: Literal["Buffer"] = "Buffer"
    child_node: "AnySpatialNodeType"
    distance: float
    distance_unit: Literal["kilometers", "meters", "miles", "nautical miles"]

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry:
        child_bounds = self.child_node.to_geometry(place_lookup)
        return add_buffer(child_bounds, _to_kilometers(self.distance_unit, self.distance))


class DirectionalConstraint(BaseModel):
    """Constrains a spatial area by a direction.

    Example: 'west of London' represents the entire world west of London
    """

    node_type: Literal["DirectionalConstraint"] = "DirectionalConstraint"
    child_node: "AnySpatialNodeType"
    direction: Literal["west", "north", "south", "east"]

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry:
        child_bounds = self.child_node.to_geometry(place_lookup)
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

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry:
        result: BaseGeometry | None = None
        for node in self.child_nodes:
            node_geom = node.to_geometry(place_lookup)

            if result:
                if result.intersects(node_geom):
                    result = result.intersection(node_geom)
                else:
                    raise GeocodeError("The two areas do not intersect")
            else:
                result = node_geom
        if result is None:
            raise Exception("Unexpected empty list of child nodes")
        return result


class Union(SpatialNodeType):
    """Represents the spatial union of two areas."""

    node_type: Literal["Union"] = "Union"
    child_nodes: "list[AnySpatialNodeType]"

    @classmethod
    def from_nodes(cls, *nodes: "AnySpatialNodeType") -> Self:
        return cls(child_nodes=list(nodes))

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry:
        result: BaseGeometry | None = None
        for node in self.child_nodes:
            node_geom = node.to_geometry(place_lookup)
            result = result.union(node_geom) if result else node_geom
        if result is None:
            raise Exception("Unexpected empty list of child nodes")
        return result


class Difference(SpatialNodeType):
    """Represents the spatial difference of two areas."""

    node_type: Literal["Difference"] = "Difference"
    child_node_1: "AnySpatialNodeType"
    child_node_2: "AnySpatialNodeType"

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry:
        b1 = self.child_node_1.to_geometry(place_lookup)
        b2 = self.child_node_2.to_geometry(place_lookup)
        return b1.difference(b2)


# FUTURE preprocess the query to change the way Between is implemented. If it's inside of another
# area that can be used as the contained bounds.


class Between(SpatialNodeType):
    """Represents a spatial area between two other areas."""

    node_type: Literal["Between"] = "Between"
    child_node_1: "AnySpatialNodeType"
    child_node_2: "AnySpatialNodeType"

    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry:
        b1 = self.child_node_1.to_geometry(place_lookup)
        b2 = self.child_node_2.to_geometry(place_lookup)
        return between(b1, b2)


AnySpatialNodeType = (
    NamedPlace
    | Buffer
    | BorderBetween
    | BorderOf
    | CoastOf
    | OffTheCoastOf
    | Intersection
    | Union
    | Difference
    | Between
    | DirectionalConstraint
)


class SpatialNode(RootModel[AnySpatialNodeType]):
    def to_geometry(self, place_lookup: PlaceLookup) -> BaseGeometry:
        return self.root.to_geometry(place_lookup)
