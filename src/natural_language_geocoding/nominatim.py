import json
from typing import Any
import requests
from e84_geoai_common.util import timed_function, get_env_var
from e84_geoai_common.geometry import geometry_from_wkt
from shapely.geometry.base import BaseGeometry


def _get_best_place(places: list[dict[str, Any]]) -> dict[str, Any]:
    """Filters the nominatim places to try and select the most relevant place"""
    for place in places:
        if not place["geotext"].startswith("POINT"):
            return place
    return places[0]


@timed_function
def nominatim_search(name: str) -> BaseGeometry | None:
    print(f"Searching for [{name}] geometry")

    nominatim_user_agent = get_env_var("NOMINATIM_USER_AGENT")

    places = requests.get(
        "https://nominatim.openstreetmap.org/search",
        params={"q": name, "format": "json", "limit": 5, "polygon_text": True},
        headers={"User-Agent": nominatim_user_agent},
    ).json()
    if len(places) > 0:
        selected_place = _get_best_place(places)
        print(f"Nominatim place found for [{name}]:", json.dumps(selected_place)[0:100])
        return geometry_from_wkt(selected_place["geotext"])
    else:
        return None
