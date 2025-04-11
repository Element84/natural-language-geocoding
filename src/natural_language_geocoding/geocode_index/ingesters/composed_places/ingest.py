"""TODO docs."""

import logging

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
from natural_language_geocoding.geocode_index.ingesters.composed_places.composers_core import (
    CompositionComponent,
    PlaceLookupComponent,
    UnionComponent,
)
from natural_language_geocoding.geocode_index.ingesters.composed_places.iberian_peninsula import (
    IberianPeninsulaCompositionComponent,
)
from natural_language_geocoding.place_lookup import PlaceSearchRequest

logger = logging.getLogger(__name__)


class Composition(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    id: str
    place_name: str
    place_type: GeoPlaceType

    component: CompositionComponent

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
            properties={"sources": [source.model_dump(mode="json") for source in composed.sources]},
        )


compositions = [
    Composition(
        id="comp_atlantic",
        place_name="Atlantic Ocean",
        place_type=GeoPlaceType.ocean,
        component=UnionComponent(
            components=[
                PlaceLookupComponent.with_name_type("North Atlantic Ocean", GeoPlaceType.ocean),
                PlaceLookupComponent.with_name_type("Sargasso Sea", GeoPlaceType.marinearea),
                PlaceLookupComponent.with_name_type("South Atlantic Ocean", GeoPlaceType.ocean),
            ]
        ),
    ),
    Composition(
        id="comp_pacific",
        place_name="Pacific Ocean",
        place_type=GeoPlaceType.ocean,
        component=UnionComponent(
            components=[
                PlaceLookupComponent.with_name_type("North Pacific Ocean", GeoPlaceType.ocean),
                PlaceLookupComponent.with_name_type("South Pacific Ocean", GeoPlaceType.ocean),
            ]
        ),
    ),
    Composition(
        id="comp_mediterranean",
        place_name="Mediterranean Sea",
        place_type=GeoPlaceType.sea,
        component=UnionComponent(
            components=[
                PlaceLookupComponent(
                    request=PlaceSearchRequest(
                        name="Mediterranean Sea",
                        place_type=GeoPlaceType.sea,
                        source_type=GeoPlaceSourceType.ne,
                    ),
                    num_to_combine=2,
                ),
                PlaceLookupComponent.with_name_type("Adriatic Sea", GeoPlaceType.sea),
                PlaceLookupComponent.with_name_type("Aegean Sea", GeoPlaceType.sea),
                PlaceLookupComponent.with_name_type("Tyrrhenian Sea", GeoPlaceType.sea),
                PlaceLookupComponent.with_name_type("Ionian Sea", GeoPlaceType.sea),
                PlaceLookupComponent.with_name_type("Balearic Sea", GeoPlaceType.sea),
                PlaceLookupComponent.with_name_type("Alboran Sea", GeoPlaceType.sea),
                PlaceLookupComponent.with_name_type("Ligurian Sea", GeoPlaceType.sea),
                PlaceLookupComponent.with_name_type("Sea of Crete", GeoPlaceType.sea),
                PlaceLookupComponent.with_name_type("Gulf of Sidra", GeoPlaceType.marinearea),
            ]
        ),
    ),
    Composition(
        id="comp_iberia",
        place_name="Iberian Peninsula",
        place_type=GeoPlaceType.peninsula,
        component=IberianPeninsulaCompositionComponent(),
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


#################################
# Code for debugging
# ruff: noqa: ERA001


# place_lookup = GeocodeIndexPlaceLookup()

# places = [comp.lookup(place_lookup) for comp in compositions]

# len(places)

# places[0].display_geometry()
# places[1].display_geometry()
# places[2].display_geometry()
# places[3].display_geometry()
