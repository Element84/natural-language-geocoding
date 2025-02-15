import json
import logging
from abc import ABC, abstractmethod
from typing import Any

import requests
from e84_geoai_common.geometry import geometry_from_wkt
from e84_geoai_common.util import get_env_var, timed_function
from shapely.geometry.base import BaseGeometry


def _get_best_place(places: list[dict[str, Any]]) -> dict[str, Any]:
    """Filters the nominatim places to try and select the most relevant place."""
    for place in places:
        if not place["geotext"].startswith("POINT"):
            return place
    return places[0]


class PlaceLookup(ABC):
    @abstractmethod
    def search(self, name: str) -> BaseGeometry: ...


class NominatimAPI(PlaceLookup):
    @timed_function
    def search(self, name: str) -> BaseGeometry:
        """Finds the geometry of a place using Nominatim."""
        logging.info("Searching for [%s] geometry", name)

        nominatim_user_agent = get_env_var("NOMINATIM_USER_AGENT")

        places = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": name, "format": "json", "limit": 5, "polygon_text": True},
            headers={"User-Agent": nominatim_user_agent},
            timeout=5,
        ).json()
        if len(places) > 0:
            selected_place = _get_best_place(places)
            logging.info(
                "Nominatim place found for [%s]: %s", name, json.dumps(selected_place)[0:100]
            )
            return geometry_from_wkt(selected_place["geotext"])
        raise LookupError(f"Unable to find place with name {name}")
