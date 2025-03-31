from natural_language_geocoding.geocode_index.geoplace import GeoPlace
from natural_language_geocoding.geocode_index.index import (
    GeocodeIndexBase,
    SearchRequest,
    SearchResponse,
)


class InMemoryGeocodeIndex(GeocodeIndexBase):
    id_to_place: dict[str, GeoPlace]

    def __init__(self) -> None:
        self.id_to_place = {}

    def create_index(self, *, recreate: bool = False) -> None:
        if recreate:
            self.id_to_place = {}

    def bulk_index(self, places: list[GeoPlace]) -> None:
        for place in places:
            self.id_to_place[place.id] = place

    def search(self, request: SearchRequest) -> SearchResponse: ...
