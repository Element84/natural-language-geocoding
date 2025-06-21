"""Defines the Iberian peninsual as a composed place."""

from shapely import LinearRing, Polygon

from natural_language_geocoding.geocode_index.geocode_index_place_lookup import (
    GeocodeIndexPlaceLookup,
)
from natural_language_geocoding.geocode_index.geoplace import GeoPlaceType
from natural_language_geocoding.geocode_index.ingesters.composed_places.composers_core import (
    ComposedPlace,
    CompositionComponent,
)
from natural_language_geocoding.place_lookup import PlaceSearchRequest

# A geometric mask that's the portion of France that's considered in the Iberian Peninsula.
france_iberian_mask_geom = Polygon(
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


class IberianPeninsulaCompositionComponent(CompositionComponent):
    """Defines the Iberian Peninsula as a place composed for various countries."""

    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> ComposedPlace:
        europe = ComposedPlace.from_request(
            place_lookup, PlaceSearchRequest(name="Europe", place_type=GeoPlaceType.continent)
        )
        spain = ComposedPlace.from_request(
            place_lookup,
            PlaceSearchRequest(
                name="Spain", place_type=GeoPlaceType.country, in_continent="Europe"
            ),
        )
        portugal = ComposedPlace.from_request(
            place_lookup,
            PlaceSearchRequest(
                name="Portugal", place_type=GeoPlaceType.country, in_continent="Europe"
            ),
        )
        france = ComposedPlace.from_request(
            place_lookup,
            PlaceSearchRequest(
                name="France", place_type=GeoPlaceType.country, in_continent="Europe"
            ),
        )
        andorra = ComposedPlace.from_request(
            place_lookup,
            PlaceSearchRequest(
                name="Andorra", place_type=GeoPlaceType.country, in_continent="Europe"
            ),
        )
        balearic = ComposedPlace.from_request(
            place_lookup,
            PlaceSearchRequest(
                name="Balearic Islands",
                place_type=GeoPlaceType.region,
                in_continent="Europe",
                in_country="Spain",
            ),
        )

        france_iberian_mask = ComposedPlace(
            place_name="France Iberian Peninsula Mask",
            geom=france_iberian_mask_geom,
        )

        spain_in_europe = spain.intersection(europe).difference(balearic)
        portugal_in_europe = portugal.intersection(europe)
        france_iberia_in_europe = france.intersection(europe).intersection(france_iberian_mask)
        andora_in_europe = andorra.intersection(europe)

        spain_port = spain_in_europe.union_at_border(portugal_in_europe)
        spain_port_andorra = spain_port.union_at_border(andora_in_europe)
        return spain_port_andorra.union_at_border(france_iberia_in_europe)
