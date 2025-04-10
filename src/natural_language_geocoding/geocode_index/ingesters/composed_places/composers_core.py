"""TODO docs."""

from abc import ABC, abstractmethod

from e84_geoai_common.debugging import display_geometry
from folium import Map
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
    """TODO docs."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    id: str
    source_type: GeoPlaceSourceType
    source_path: str

    @staticmethod
    def from_place(place: GeoPlace) -> "GeoPlaceSource":
        return GeoPlaceSource(
            id=place.id, source_type=place.source.source_type, source_path=place.source.source_path
        )


class ComposedPlace(BaseModel):
    """TODO docs."""

    model_config = ConfigDict(
        strict=True, extra="forbid", frozen=True, arbitrary_types_allowed=True
    )

    # For debugging purposes
    place_name: str

    geom: BaseGeometry
    # TODO this should be a set and remove hierarchies that are a parent of a lower hierarchy
    hierarchies: list[Hierarchy] = Field(default_factory=list)
    # TODO this should be a set
    sources: list[GeoPlaceSource] = Field(default_factory=list)

    def display_geometry(self) -> Map:
        """TODO docs. Note for debugging."""
        return display_geometry([self.geom])

    @staticmethod
    def from_place(place: GeoPlace) -> "ComposedPlace":
        return ComposedPlace(
            place_name=place.place_name,
            geom=place.geom,
            hierarchies=place.hierarchies,
            sources=[GeoPlaceSource.from_place(place)],
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
            sources = [*sources, GeoPlaceSource.from_place(place)]
        return ComposedPlace(
            place_name=f"Union of [{self.place_name}] and [{place.place_name}]",
            geom=self.geom.union(place.geom),
            hierarchies=[*self.hierarchies, *place.hierarchies],
            sources=sources,
        )

    def intersection(self, place: "GeoPlace | ComposedPlace") -> "ComposedPlace":
        sources = self.sources
        if isinstance(place, GeoPlace):
            sources = [*sources, GeoPlaceSource.from_place(place)]
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

    def union_at_border(self, other: "ComposedPlace", mask: "ComposedPlace") -> "ComposedPlace":
        """TODO docs. Note it covers the border."""
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
            hierarchies=[*self.hierarchies, *other.hierarchies, *mask.hierarchies],
            sources=[*self.sources, *other.sources, *mask.sources],
        )


class CompositionComponent(BaseModel, ABC):
    """TODO docs."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    @abstractmethod
    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> ComposedPlace: ...


class PlaceLookupComponent(CompositionComponent):
    request: PlaceSearchRequest

    # TODO document that this will take the top N and combine them
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
