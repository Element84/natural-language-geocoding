"""Composition of geographic places for complex spatial queries and operations."""

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.geocode_index.geocode_index_place_lookup import (
    GeocodeIndexPlaceLookup,
)
from natural_language_geocoding.geocode_index.geoplace import (
    GeoPlace,
    GeoPlaceSourceType,
    GeoPlaceType,
    Hierarchy,
)
from natural_language_geocoding.models import border_between
from natural_language_geocoding.place_lookup import PlaceSearchRequest


class GeoPlaceSource(BaseModel):
    """Represents the source of a geoplace."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    id: str
    source_type: GeoPlaceSourceType | str
    source_path: str

    def __hash__(self) -> int:
        return hash((self.id, self.source_type, self.source_path))

    @staticmethod
    def from_place(place: GeoPlace) -> "GeoPlaceSource":
        return GeoPlaceSource(
            id=place.id, source_type=place.source.source_type, source_path=place.source.source_path
        )


class ComposedPlace(BaseModel):
    """A geographic place that can be composed from multiple sources through geometric operations.

    ComposedPlace allows for complex spatial queries by combining, intersecting, or modifying
    geographic places. It maintains references to source places and supports operations like
    union, intersection, and difference.
    """

    model_config = ConfigDict(
        strict=True, extra="forbid", frozen=True, arbitrary_types_allowed=True
    )

    # For debugging purposes
    place_name: str

    geom: BaseGeometry
    # FUTURE ideally this should be a set and remove hierarchies that are a parent of a lower
    # hierarchy.
    hierarchies: list[Hierarchy] = Field(default_factory=list[Hierarchy])
    sources: set[GeoPlaceSource] = Field(default_factory=set[GeoPlaceSource])

    def display_geometry(self) -> Any:  # noqa: ANN401
        """Displays the geometry of this place on a map when run in a Jupyter Notebook."""
        from e84_geoai_common.debugging import display_geometry  # noqa: PLC0415

        return display_geometry([self.geom])

    @staticmethod
    def from_place(place: GeoPlace) -> "ComposedPlace":
        return ComposedPlace(
            place_name=place.place_name,
            geom=place.geom,
            hierarchies=place.hierarchies,
            sources={GeoPlaceSource.from_place(place)},
        )

    @staticmethod
    def from_request(
        place_lookup: GeocodeIndexPlaceLookup,
        request: PlaceSearchRequest,
        *,
        num_to_combine: int = 1,
    ) -> "ComposedPlace":
        resp = place_lookup.search_for_places(request, limit=num_to_combine)
        composed: ComposedPlace | None = None
        for place in resp.places:
            composed = (
                ComposedPlace.from_place(place) if composed is None else composed.union(place)
            )
        if composed is None:
            raise Exception(f"Unable to find places with request {request.model_dump_json()}")
        return composed

    def union(self, place: "GeoPlace | ComposedPlace") -> "ComposedPlace":
        sources = self.sources
        if isinstance(place, GeoPlace):
            sources = {*sources, GeoPlaceSource.from_place(place)}
        return ComposedPlace(
            place_name=f"Union of [{self.place_name}] and [{place.place_name}]",
            geom=self.geom.union(place.geom),
            hierarchies=[*self.hierarchies, *place.hierarchies],
            sources=sources,
        )

    def intersection(self, place: "GeoPlace | ComposedPlace") -> "ComposedPlace":
        sources = self.sources
        if isinstance(place, GeoPlace):
            sources = {*sources, GeoPlaceSource.from_place(place)}
        return ComposedPlace(
            place_name=f"Intersection of [{self.place_name}] and [{place.place_name}]",
            geom=self.geom.intersection(place.geom),
            hierarchies=[*self.hierarchies, *place.hierarchies],
            sources=sources,
        )

    def difference(self, place: "GeoPlace | ComposedPlace") -> "ComposedPlace":
        return ComposedPlace(
            place_name=f"Difference of [{self.place_name}] and [{place.place_name}]",
            geom=self.geom.difference(place.geom),
            hierarchies=self.hierarchies,
            sources=self.sources,
        )

    def union_at_border(self, other: "ComposedPlace") -> "ComposedPlace":
        """Creates a union of two places that includes their shared border.

        This method finds the border between two places and includes it in the resulting
        geometry, ensuring complete coverage of the combined area including boundary regions.

        Args:
            other: The other ComposedPlace to union with

        Returns:
            A new ComposedPlace representing the union including the border

        Raises:
            Exception: If no border is found between the two places
        """
        combined = self.geom.union(other.geom)
        border = border_between(self.geom, other.geom)
        if border is None:
            raise Exception(
                f"Border is not found between [{self.place_name}] and [{other.place_name}]"
            )
        combined_with_border_cover = combined.union(border)
        return ComposedPlace(
            place_name=f"Union at Border of [{self.place_name}] and [{other.place_name}]",
            geom=combined_with_border_cover,
            hierarchies=[*self.hierarchies, *other.hierarchies],
            sources={*self.sources, *other.sources},
        )


class CompositionComponent(BaseModel, ABC):
    """Abstract base class for components that can be composed to create complex geographic places.

    CompositionComponent defines the interface for objects that can perform place lookups
    and return ComposedPlace objects. Concrete implementations handle different types of
    spatial operations and place queries.
    """

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    @abstractmethod
    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> ComposedPlace:
        """Performs a place lookup operation and returns a ComposedPlace.

        Args:
            place_lookup: The geocode index to search for places

        Returns:
            A ComposedPlace representing the result of the lookup operation
        """
        ...


class PlaceLookupComponent(CompositionComponent):
    request: PlaceSearchRequest

    # This will take the top N and combine them
    num_to_combine: int = 1

    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> ComposedPlace:
        return ComposedPlace.from_request(
            place_lookup, self.request, num_to_combine=self.num_to_combine
        )

    @staticmethod
    def with_name_type(name: str, place_type: GeoPlaceType) -> "PlaceLookupComponent":
        return PlaceLookupComponent(request=PlaceSearchRequest(name=name, place_type=place_type))


class IntersectionComponent(CompositionComponent):
    components: list[CompositionComponent]

    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> ComposedPlace:
        result: ComposedPlace | None = None
        for component in self.components:
            composed = component.lookup(place_lookup)
            result = composed if result is None else result.intersection(composed)
        if result is None:
            raise Exception("Unexpected None result")
        return result


class UnionComponent(CompositionComponent):
    components: list[CompositionComponent]

    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> ComposedPlace:
        result: ComposedPlace | None = None
        for component in self.components:
            composed = component.lookup(place_lookup)
            result = composed if result is None else result.union(composed)
        if result is None:
            raise Exception("Unexpected None result")
        return result


class ContinentSubregion(CompositionComponent):
    continent: str
    countries: list[str]
    constrain_to_continent: bool = False

    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> ComposedPlace:
        countries = [
            ComposedPlace.from_request(
                place_lookup,
                PlaceSearchRequest(
                    name=country, place_type=GeoPlaceType.country, in_continent=self.continent
                ),
            )
            for country in self.countries
        ]
        # Clip the country to only the parts that are in the continent
        if self.constrain_to_continent:
            continent = ComposedPlace.from_request(
                place_lookup,
                PlaceSearchRequest(name=self.continent, place_type=GeoPlaceType.continent),
            )
            countries = [country.intersection(continent) for country in countries]

        # Merges things together like bubble sort but is likely closer to O(log N)
        merged: ComposedPlace = countries[0]
        left_to_merge = countries[1:]
        while len(left_to_merge) > 0:
            remaining: list[ComposedPlace] = []
            for country in left_to_merge:
                if country.geom.intersects(merged.geom):
                    merged = merged.union_at_border(country)
                else:
                    remaining.append(country)

            if len(remaining) == len(left_to_merge):
                # Nothing was actually merged in. The remaining areas don't intersect so we'll union
                # them together.
                for to_merge in remaining:
                    merged = merged.union(to_merge)
                remaining = []  # Nothing left to merge
            left_to_merge = remaining
        return merged


###########
# Code for manual testing
# ruff: noqa: ERA001

# place_lookup = GeocodeIndexPlaceLookup()

# subregion = ContinentSubregion(
#     continent="Africa", countries=["Morocco", "Algeria", "Tunisia", "Libya", "Egypt", "Sudan"]
# )
# composed = subregion.lookup(place_lookup)

# composed.display_geometry()
