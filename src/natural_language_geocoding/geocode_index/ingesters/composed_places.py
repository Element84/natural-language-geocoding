"""TODO docs."""

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

from natural_language_geocoding.geocode_index.geocode_index_place_lookup import (
    GeocodeIndexPlaceLookup,
)
from natural_language_geocoding.geocode_index.geoplace import (
    GeoPlace,
    GeoPlaceSource,
    GeoPlaceSourceType,
    GeoPlaceType,
)
from natural_language_geocoding.geocode_index.index import GeocodeIndex
from natural_language_geocoding.place_lookup import PlaceSearchRequest

if TYPE_CHECKING:
    from shapely.geometry.base import BaseGeometry

logger = logging.getLogger(__name__)


class _CompositionComponent(BaseModel, ABC):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    @abstractmethod
    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> list[GeoPlace]: ...


class _PlaceLookupComponent(_CompositionComponent):
    request: PlaceSearchRequest
    num_to_take: int = 1

    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> list[GeoPlace]:
        return place_lookup.search_for_places_raw(
            self.request,
            limit=self.num_to_take,
        ).places

    @staticmethod
    def with_name_type(name: str, place_type: GeoPlaceType) -> "_PlaceLookupComponent":
        return _PlaceLookupComponent(request=PlaceSearchRequest(name=name, place_type=place_type))


class _Composition(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    id: str
    place_name: str
    place_type: GeoPlaceType

    components: list[_CompositionComponent]

    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> GeoPlace:
        logger.info(
            "Composing together places for %s %s %s", self.id, self.place_name, self.place_type
        )
        component_places = [
            place for component in self.components for place in component.lookup(place_lookup)
        ]

        combo_geom: BaseGeometry | None = None
        area: float | None = 0
        population: int | None = 0

        for place in component_places:
            combo_geom = place.geom if combo_geom is None else combo_geom.union(place.geom)
            if place.area_sq_km and area:
                area += place.area_sq_km
            elif place.area_sq_km is None:
                area = None
            if place.population and population:
                population += place.population
            elif place.population is None:
                population = None

        if combo_geom is None:
            raise Exception(
                f"Unexpected empty geometry for composition {self.id} {self.place_name}"
            )

        hierarchies = [h for p in component_places for h in p.hierarchies]

        return GeoPlace(
            id=self.id,
            place_name=self.place_name,
            type=self.place_type,
            geom=combo_geom,
            source=GeoPlaceSource(source_type=GeoPlaceSourceType.comp, source_path="composed"),
            area_sq_km=area,
            population=population,
            hierarchies=hierarchies,
            properties={
                "sources": [
                    {
                        "id": place.id,
                        "source_type": place.source.source_type.value,
                        "source_path": place.source.source_path,
                    }
                    for place in component_places
                ]
            },
        )


compositions = [
    _Composition(
        id="comp_atlantic",
        place_name="Atlantic Ocean",
        place_type=GeoPlaceType.ocean,
        components=[
            _PlaceLookupComponent.with_name_type("North Atlantic Ocean", GeoPlaceType.ocean),
            _PlaceLookupComponent.with_name_type("Sargasso Sea", GeoPlaceType.marinearea),
            _PlaceLookupComponent.with_name_type("South Atlantic Ocean", GeoPlaceType.ocean),
        ],
    ),
    _Composition(
        id="comp_pacific",
        place_name="Pacific Ocean",
        place_type=GeoPlaceType.ocean,
        components=[
            _PlaceLookupComponent.with_name_type("North Pacific Ocean", GeoPlaceType.ocean),
            _PlaceLookupComponent.with_name_type("South Pacific Ocean", GeoPlaceType.ocean),
        ],
    ),
    _Composition(
        id="comp_mediterranean",
        place_name="Mediterranean Sea",
        place_type=GeoPlaceType.ocean,
        components=[
            _PlaceLookupComponent(
                request=PlaceSearchRequest(
                    name="Mediterranean Sea",
                    place_type=GeoPlaceType.sea,
                    source_type=GeoPlaceSourceType.ne,
                ),
                num_to_take=2,
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
        ],
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
