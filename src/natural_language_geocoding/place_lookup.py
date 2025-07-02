import json
import logging
from abc import ABC, abstractmethod
from typing import Any

import requests
from e84_geoai_common.geometry import geometry_from_wkt
from e84_geoai_common.util import get_env_var, timed_function
from pydantic import BaseModel, ConfigDict
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.errors import GeocodeError
from natural_language_geocoding.geocode_index.geoplace import GeoPlaceSourceType, GeoPlaceType


def _get_best_place(places: list[dict[str, Any]]) -> dict[str, Any]:
    """Filters the nominatim places to try and select the most relevant place."""
    for place in places:
        if not place["geotext"].startswith("POINT"):
            return place
    return places[0]


class PlaceSearchRequest(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    name: str
    place_type: GeoPlaceType | str | None = None
    in_continent: str | None = None
    in_country: str | None = None
    in_region: str | None = None
    source_type: GeoPlaceSourceType | str | None = None

    @property
    def place_type_value(self) -> str | None:
        if self.place_type is None:
            return None
        return self.place_type if isinstance(self.place_type, str) else self.place_type.value


class PlaceLookup(ABC):
    @abstractmethod
    def search(self, request: PlaceSearchRequest) -> BaseGeometry: ...


class NominatimAPI(PlaceLookup):
    """Uses the OpenStreetMap API, Nominatim, as a source of places.

    This is deprecated. Use the GeocodeIndexPlaceLookup instead.
    """

    logger = logging.getLogger(f"{__name__}.{__qualname__}")

    @timed_function(logger)
    def search(self, request: PlaceSearchRequest) -> BaseGeometry:
        """Finds the geometry of a place using Nominatim."""
        name = request.name
        self.logger.info("Searching for [%s] geometry", name)

        nominatim_user_agent = get_env_var("NOMINATIM_USER_AGENT")

        places = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": name, "format": "json", "limit": 5, "polygon_text": True},
            headers={"User-Agent": nominatim_user_agent},
            timeout=5,
        ).json()
        if len(places) > 0:
            selected_place = _get_best_place(places)
            self.logger.info(
                "Nominatim place found for [%s]: %s", name, json.dumps(selected_place)[0:100]
            )
            return geometry_from_wkt(selected_place["geotext"])
        raise GeocodeError(f"Unable to find place with name {name}")
