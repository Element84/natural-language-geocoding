"""Provides an implementation of Place Lookup for lunar features using OpenSearch."""

import logging
from typing import Any

from e84_geoai_common.util import timed_function
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.errors import GeocodeError
from natural_language_geocoding.geocode_index.index import (
    SearchRequest,
    SearchResponse,
    SortField,
)
from natural_language_geocoding.geocode_index.lunar_index import (
    LunarGeocodeIndex,
    LunarPlaceIndexField,
)
from natural_language_geocoding.geocode_index.lunar_place_types import (
    DEFAULT_LUNAR_PLACE_TYPE_SORT_ORDER,
    LunarPlaceType,
)
from natural_language_geocoding.geocode_index.opensearch_utils import (
    QueryCondition,
    QueryDSL,
    ordered_values_to_sort_cond,
)
from natural_language_geocoding.place_lookup import PlaceLookup, PlaceSearchRequest


class LunarPlaceLookup(PlaceLookup):
    """Implements PlaceLookup for lunar features using OpenSearch."""

    logger = logging.getLogger(f"{__name__}.{__qualname__}")

    _index: LunarGeocodeIndex
    _type_sort_cond: dict[str, Any]

    def __init__(
        self,
        index: LunarGeocodeIndex | None = None,
        *,
        place_type_sort_order: list[LunarPlaceType | str] = DEFAULT_LUNAR_PLACE_TYPE_SORT_ORDER,
    ) -> None:
        self._index = index or LunarGeocodeIndex()

        self._type_sort_cond = ordered_values_to_sort_cond(
            LunarPlaceIndexField.type, place_type_sort_order
        )

    def create_search_request(
        self,
        request: PlaceSearchRequest,
        *,
        limit: int = 5,
        explain: bool = False,
    ) -> SearchRequest:
        should_conds: list[QueryCondition] = []
        must_conds: list[QueryCondition] = []
        must_not_conds: list[QueryCondition] = []

        if request.place_type_value:
            should_conds.append(
                QueryDSL.term(LunarPlaceIndexField.type, request.place_type_value)
            )

        # Lunar features don't use continent/country/region hierarchy
        # so we skip those filters entirely

        # Name matching - dis_max for best score from multiple match strategies
        name_match = QueryDSL.dis_max(
            QueryDSL.term(
                LunarPlaceIndexField.place_name_lower_keyword, request.name, boost=10.0
            ),
            QueryDSL.term(
                LunarPlaceIndexField.alternate_names_lower_keyword, request.name, boost=5.0
            ),
            QueryDSL.match(LunarPlaceIndexField.place_name, request.name, fuzzy=True, boost=2.0),
            QueryDSL.match(
                LunarPlaceIndexField.alternate_names, request.name, fuzzy=True, boost=1.0
            ),
        )
        must_conds.append(name_match)

        return SearchRequest(
            size=limit,
            query=QueryDSL.bool_cond(
                must_conds=must_conds, should_conds=should_conds, must_not_conds=must_not_conds
            ),
            sort=[
                SortField(field="_score", order="desc"),
                self._type_sort_cond,
            ],
            explain=explain,
        )

    @timed_function(logger)
    def search_for_places(
        self,
        request: PlaceSearchRequest,
        *,
        limit: int = 5,
        explain: bool = False,
    ) -> SearchResponse:
        """Searches for lunar places and returns the most likely matches."""
        search_request = self.create_search_request(request, limit=limit, explain=explain)
        return self._index.search(search_request)

    def search(
        self,
        request: PlaceSearchRequest,
    ) -> BaseGeometry:
        """Searches for lunar places and returns the most likely geometry."""
        search_resp = self.search_for_places(request)
        places = search_resp.places
        if len(places) > 0:
            return places[0].geom
        raise GeocodeError(
            f"Unable to find lunar place with name [{request.name}] type [{request.place_type}]"
        )
