"""TODO docs."""

import logging
from abc import ABC, abstractmethod

from pydantic import BaseModel, ConfigDict
from shapely import LinearRing, Polygon
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.geocode_index.geocode_index_place_lookup import (
    GeocodeIndexPlaceLookup,
)
from natural_language_geocoding.geocode_index.geoplace import (
    GeoPlace,
    GeoPlaceSource,
    GeoPlaceSourceType,
    GeoPlaceType,
    Hierarchy,
)
from natural_language_geocoding.geocode_index.index import GeocodeIndex
from natural_language_geocoding.models import border_between
from natural_language_geocoding.place_lookup import PlaceSearchRequest

logger = logging.getLogger(__name__)

# A mask that when intersected with france defines the portion of it on the iberian peninsula
france_iberian_mask = Polygon(
    LinearRing(
        [
            (4.0894268, 43.5549243),
            (-1.0552007, 45.5946521),
            (-1.1159623, 45.5661185),
            (-1.3805183, 45.1258235),
            (-2.0772539, 43.1871382),
            (3.0677654, 41.6975253),
            (4.0894268, 43.5549243),
        ]
    )
)


class _GeoPlaceSource(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    id: str
    source_type: GeoPlaceSourceType
    source_path: str

    @staticmethod
    def from_place(place: GeoPlace) -> "_GeoPlaceSource":
        return _GeoPlaceSource(
            id=place.id, source_type=place.source.source_type, source_path=place.source.source_path
        )


class _ComposedPlace(BaseModel):
    model_config = ConfigDict(
        strict=True, extra="forbid", frozen=True, arbitrary_types_allowed=True
    )

    geom: BaseGeometry
    # TODO this should be a set and remove hierarchies that are a parent of a lower hierarchy
    hierarchies: list[Hierarchy]
    # TODO this should be a set
    sources: list[_GeoPlaceSource]

    @staticmethod
    def from_place(place: GeoPlace) -> "_ComposedPlace":
        return _ComposedPlace(
            geom=place.geom,
            hierarchies=place.hierarchies,
            sources=[_GeoPlaceSource.from_place(place)],
        )

    @staticmethod
    def from_request(
        place_lookup: GeocodeIndexPlaceLookup,
        request: PlaceSearchRequest,
        *,
        num_to_combine: int = 1,
    ) -> "_ComposedPlace":
        resp = place_lookup.search_for_places_raw(request, limit=num_to_combine)
        composed: _ComposedPlace | None = None
        for place in resp.places:
            composed = (
                _ComposedPlace.from_place(place) if composed is None else composed.union(place)
            )
        if composed is None:
            raise Exception(f"Unable to find places with request {request.model_dump_json()}")
        return composed

    def union(self, place: "GeoPlace | _ComposedPlace") -> "_ComposedPlace":
        sources = self.sources
        if isinstance(place, GeoPlace):
            sources = [*sources, _GeoPlaceSource.from_place(place)]
        return _ComposedPlace(
            geom=self.geom.union(place.geom),
            hierarchies=[*self.hierarchies, *place.hierarchies],
            sources=sources,
        )

    def intersection(self, place: "GeoPlace | _ComposedPlace") -> "_ComposedPlace":
        sources = self.sources
        if isinstance(place, GeoPlace):
            sources = [*sources, _GeoPlaceSource.from_place(place)]
        return _ComposedPlace(
            geom=self.geom.intersection(place.geom),
            hierarchies=[*self.hierarchies, *place.hierarchies],
            sources=sources,
        )

    def union_at_border(self, other: "_ComposedPlace", mask: "_ComposedPlace") -> "_ComposedPlace":
        """TODO docs. Note it covers the border"""
        combined = self.geom.union(other.geom)
        border = border_between(self.geom, other.geom)
        if border is None:
            raise Exception("Borders is none unexpectedly")
        combined_with_border_cover = combined.union(border)
        return _ComposedPlace(
            geom=combined_with_border_cover,
            hierarchies=[*self.hierarchies, *other.hierarchies, *mask.hierarchies],
            sources=[*self.sources, *other.sources, *mask.sources],
        )


class _CompositionComponent(BaseModel, ABC):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    @abstractmethod
    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> _ComposedPlace: ...


class _PlaceLookupComponent(_CompositionComponent):
    request: PlaceSearchRequest

    # TODO document that this will take the top N and combine them
    num_to_combine: int = 1

    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> _ComposedPlace:
        return _ComposedPlace.from_request(
            place_lookup, self.request, num_to_combine=self.num_to_combine
        )

    @staticmethod
    def with_name_type(name: str, place_type: GeoPlaceType) -> "_PlaceLookupComponent":
        return _PlaceLookupComponent(request=PlaceSearchRequest(name=name, place_type=place_type))


class _IntersectionComponent(_CompositionComponent):
    components: list[_CompositionComponent]

    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> _ComposedPlace:
        result: _ComposedPlace | None = None
        for component in self.components:
            composed = component.lookup(place_lookup)
            result = composed if result is None else result.intersection(composed)
        if result is None:
            raise Exception("Unexpected None result")
        return result


class _CountriesWithinContinentComponent(_CompositionComponent):
    countries: list[str]
    continent: str

    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> _ComposedPlace:
        continent = _ComposedPlace.from_request(
            place_lookup, PlaceSearchRequest(name=self.continent, place_type=GeoPlaceType.continent)
        )
        countries = [
            _ComposedPlace.from_request(
                place_lookup,
                PlaceSearchRequest(
                    name=country, place_type=GeoPlaceType.country, in_continent=self.continent
                ),
            )
            for country in self.countries
        ]
        countries_within_continent = [country.intersection(continent) for country in countries]
        # TODO see if you can refactor code to make this more reduce like to make code simpler
        result: _ComposedPlace | None = None
        for country in countries_within_continent:
            result = country if result is None else result.union_at_border(country, continent)
        if result is None:
            raise Exception("result unexpectedly None")
        return result


class _UnionComponent(_CompositionComponent):
    components: list[_CompositionComponent]

    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> _ComposedPlace:
        result: _ComposedPlace | None = None
        for component in self.components:
            composed = component.lookup(place_lookup)
            result = composed if result is None else result.union(composed)
        if result is None:
            raise Exception("Unexpected None result")
        return result


# TODO rename this class
class _Composition(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    id: str
    place_name: str
    place_type: GeoPlaceType

    component: _CompositionComponent

    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> GeoPlace:
        logger.info(
            "Composing together places for %s %s %s", self.id, self.place_name, self.place_type
        )
        composed = self.component.lookup(place_lookup)

        return GeoPlace(
            id=self.id,
            place_name=self.place_name,
            type=self.place_type,
            geom=composed.geom,
            source=GeoPlaceSource(source_type=GeoPlaceSourceType.comp, source_path="composed"),
            hierarchies=composed.hierarchies,
            properties={"sources": [source.model_dump() for source in composed.sources]},
        )


compositions = [
    _Composition(
        id="comp_atlantic",
        place_name="Atlantic Ocean",
        place_type=GeoPlaceType.ocean,
        component=_UnionComponent(
            components=[
                _PlaceLookupComponent.with_name_type("North Atlantic Ocean", GeoPlaceType.ocean),
                _PlaceLookupComponent.with_name_type("Sargasso Sea", GeoPlaceType.marinearea),
                _PlaceLookupComponent.with_name_type("South Atlantic Ocean", GeoPlaceType.ocean),
            ]
        ),
    ),
    _Composition(
        id="comp_pacific",
        place_name="Pacific Ocean",
        place_type=GeoPlaceType.ocean,
        component=_UnionComponent(
            components=[
                _PlaceLookupComponent.with_name_type("North Pacific Ocean", GeoPlaceType.ocean),
                _PlaceLookupComponent.with_name_type("South Pacific Ocean", GeoPlaceType.ocean),
            ]
        ),
    ),
    _Composition(
        id="comp_mediterranean",
        place_name="Mediterranean Sea",
        place_type=GeoPlaceType.sea,
        component=_UnionComponent(
            components=[
                _PlaceLookupComponent(
                    request=PlaceSearchRequest(
                        name="Mediterranean Sea",
                        place_type=GeoPlaceType.sea,
                        source_type=GeoPlaceSourceType.ne,
                    ),
                    num_to_combine=2,
                ),
                _PlaceLookupComponent.with_name_type("Adriatic Sea", GeoPlaceType.sea),
                _PlaceLookupComponent.with_name_type("Aegean Sea", GeoPlaceType.sea),
                _PlaceLookupComponent.with_name_type("Tyrrhenian Sea", GeoPlaceType.sea),
                _PlaceLookupComponent.with_name_type("Ionian Sea", GeoPlaceType.sea),
                _PlaceLookupComponent.with_name_type("Balearic Sea", GeoPlaceType.sea),
                _PlaceLookupComponent.with_name_type("Alboran Sea", GeoPlaceType.sea),
                _PlaceLookupComponent.with_name_type("Ligurian Sea", GeoPlaceType.sea),
                _PlaceLookupComponent.with_name_type("Sea of Crete", GeoPlaceType.sea),
                _PlaceLookupComponent.with_name_type("Gulf of Sidra", GeoPlaceType.marinearea),
            ]
        ),
    ),
    _Composition(
        id="comp_iberia",
        place_name="Iberian Peninsula",
        place_type=GeoPlaceType.peninsula,
        component=_CountriesWithinContinentComponent(
            continent="Europe", countries=["Portugal", "Spain", "France", "Andora"]
        ),
    ),
]


def ingest_compositions() -> None:
    """TODO docs."""
    place_lookup = GeocodeIndexPlaceLookup()
    logger.info("Generating combined compositions")
    places = [comp.lookup(place_lookup) for comp in compositions]
    index = GeocodeIndex()
    logger.info("Indexing compositions")
    index.bulk_index(places)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    ingest_compositions()

####################
# Code for manual testing

place_lookup = GeocodeIndexPlaceLookup()

from e84_geoai_common.debugging import display_geometry  # noqa: E402

comp = _Composition(
    id="comp_iberia",
    place_name="Iberian Peninsula",
    place_type=GeoPlaceType.peninsula,
    component=_CountriesWithinContinentComponent(
        continent="Europe", countries=["Portugal", "Spain", "France", "Andora"]
    ),
)

place = comp.lookup(place_lookup)


europe = place_lookup.search(PlaceSearchRequest(name="Europe", place_type=GeoPlaceType.continent))
spain = place_lookup.search(
    PlaceSearchRequest(name="Spain", place_type=GeoPlaceType.country, in_continent="Europe")
)
portugal = place_lookup.search(
    PlaceSearchRequest(name="Portugal", place_type=GeoPlaceType.country, in_continent="Europe")
)
france = place_lookup.search(
    PlaceSearchRequest(name="France", place_type=GeoPlaceType.country, in_continent="Europe")
)
balearic = place_lookup.search(
    PlaceSearchRequest(
        name="Balearic Islands",
        place_type=GeoPlaceType.region,
        in_continent="Europe",
        in_country="Spain",
    )
)

display_geometry([france_iberian_mask])


display_geometry([_union_bordering_areas(france, spain, europe)])


display_geometry([portugal_spain_border_cover])

spain_in_europe = europe.intersection(spain).difference(balearic)
france_in_europe = europe.intersection(france)


portugal_in_europe = europe.intersection(portugal)
france_iberia = france_iberian_mask.intersection(france)
iberia = (
    spain_in_europe.union(portugal_in_europe)
    .union(portugal_spain_border_cover)
    .union(france_iberia)
)


display_geometry([europe])
display_geometry([spain])
display_geometry([spain_in_europe])
display_geometry([portugal_in_europe])
display_geometry([france_iberia])
display_geometry([iberia])


display_geometry([place_lookup.search(PlaceSearchRequest(name="Mallorca", in_country="Spain"))])
