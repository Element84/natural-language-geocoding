import json
from pathlib import Path

from e84_geoai_common.geometry import geometry_from_geojson_dict
from shapely import Polygon
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.place_lookup import PlaceLookup

# states_to_geom.json was generated from simplified Census cartographic boundaries for US states.
_STATES_TO_GEOM_FILE = Path(__file__).parent / "states_to_geom.json"


with _STATES_TO_GEOM_FILE.open() as f:
    parsed = json.load(f)

_STATES_TO_GEOMS = {state: geometry_from_geojson_dict(geojson) for state, geojson in parsed.items()}


FLORIDA = _STATES_TO_GEOMS["Florida"]
GEORGIA = _STATES_TO_GEOMS["Georgia"]
ALABAMA = _STATES_TO_GEOMS["Georgia"]
MISSISSIPPI = _STATES_TO_GEOMS["Mississippi"]
LOUISIANA = _STATES_TO_GEOMS["Louisiana"]
TEXAS = _STATES_TO_GEOMS["Texas"]


#     1  3   4   6      9  10    13   15
#  11 ┌───────────────────────────────┐
#     │Alpha                          │
#  9  │  ┌───────┐      ┌─────────┐   │
#     │  │Bravo  │      │Gamma    │   │
#  7  │  │   ┌─────────────┐      │   │
#     │  │   │   │ Delta│  │      │   │
#     │  │   │   │      │  │      │   │
#  5  │  │   └─────────────┘      │   │
#     │  │       │      │         │   │
#  3  │  └───────┘      └─────────┘   │
#     │                               │
#  1  └───────────────────────────────┘


# Create the polygons using (x,y) coordinates
# fmt: off
ALPHA = Polygon([
    (1, 1), (15, 1),   # bottom edge
    (15, 11), (1, 11), # top edge
    (1, 1)             # back to start
])

BRAVO = Polygon([
    (3, 3), (6, 3),    # bottom edge
    (6, 9), (3, 9),    # top edge
    (3, 3)             # back to start
])

GAMMA = Polygon([
    (9, 3), (13, 3),  # bottom edge
    (13, 9), (9, 9),  # top edge
    (9, 3)            # back to start
])

DELTA = Polygon([
    (4, 5), (10, 5),   # bottom edge
    (10, 7), (4, 7),   # top edge
    (4, 5)             # back to start
])

# fmt: on


class CannedPlaceLookup(PlaceLookup):
    """Implements place lookup using a set of known geometries in a file."""

    name_to_geom: dict[str, BaseGeometry]

    def __init__(self) -> None:
        super().__init__()
        self.name_to_geom = {
            **_STATES_TO_GEOMS,
            "alpha": ALPHA,
            "bravo": BRAVO,
            "gamma": GAMMA,
            "delta": DELTA,
        }

    def search(self, name: str) -> BaseGeometry:
        if name in self.name_to_geom:
            return self.name_to_geom[name]
        raise LookupError(f"Unable to find location with name {name}")
