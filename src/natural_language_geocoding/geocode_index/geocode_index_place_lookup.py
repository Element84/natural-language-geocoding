"""TODO document this module."""

import logging

from e84_geoai_common.util import timed_function
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.errors import GeocodeError
from natural_language_geocoding.geocode_index.geoplace import (
    PLACE_TYPE_SORT_ORDER,
    SOURCE_TYPE_SORT_ORDER,
    GeoPlaceType,
)
from natural_language_geocoding.geocode_index.hierachical_place_cache import PlaceCache
from natural_language_geocoding.geocode_index.index import (
    GeocodeIndex,
    GeoPlaceIndexField,
    SearchRequest,
    SearchResponse,
    SortField,
)
from natural_language_geocoding.geocode_index.opensearch_utils import (
    QueryCondition,
    QueryDSL,
    ordered_values_to_sort_cond,
)
from natural_language_geocoding.place_lookup import PlaceLookup, PlaceSearchRequest

# Note that using a script for sorting is slow. Eventually we should switch this to an indexed id
# to improve performance.

_TYPE_SORT_COND = ordered_values_to_sort_cond(
    GeoPlaceIndexField.type, [pt.value for pt in PLACE_TYPE_SORT_ORDER]
)
_SOURCE_TYPE_SORT_COND = ordered_values_to_sort_cond(
    GeoPlaceIndexField.type, [st.value for st in SOURCE_TYPE_SORT_ORDER]
)


def _continent_country_region_to_conditions(
    place_cache: PlaceCache,
    continent_name: str | None = None,
    country_name: str | None = None,
    region_name: str | None = None,
) -> list[QueryCondition]:
    """Create query conditions to limit found places to any specified continent, country, or region."""
    should_conds: list[QueryCondition] = []
    continent_ids: set[str] | None = None
    country_ids: set[str] | None = None

    if continent_name:
        continent_ids = place_cache.find_ids(name=continent_name, place_type=GeoPlaceType.continent)
        if len(continent_ids) == 0:
            raise ValueError(f"Unable to find continent with name [{continent_name}]")
        if len(continent_ids) > 1:
            raise Exception(f"Unexpectedly found multiple continents with name [{continent_name}]")
        should_conds.append(
            QueryDSL.term(GeoPlaceIndexField.hierarchies_continent_id, next(iter(continent_ids)))
        )

    if country_name:
        country_ids = place_cache.find_ids(
            name=country_name, place_type=GeoPlaceType.country, continent_ids=continent_ids
        )
        if len(country_ids) == 0:
            raise ValueError(f"Unable to find country with name [{country_name}]")
        should_conds.append(
            QueryDSL.terms(GeoPlaceIndexField.hierarchies_country_id, list(country_ids))
        )

    if region_name:
        region_ids = place_cache.find_ids(
            name=region_name,
            place_type=GeoPlaceType.region,
            continent_ids=continent_ids,
            country_ids=country_ids,
        )

        if len(region_ids) == 0:
            raise ValueError(f"Unable to find region with name [{region_name}]")
        should_conds.append(
            QueryDSL.terms(GeoPlaceIndexField.hierarchies_region_id, list(region_ids))
        )
    return should_conds


class GeocodeIndexPlaceLookup(PlaceLookup):
    """TODO docs."""

    logger = logging.getLogger(f"{__name__}.{__qualname__}")

    _index: GeocodeIndex
    _place_cache: PlaceCache

    def __init__(self) -> None:
        self._index = GeocodeIndex()
        self._place_cache = PlaceCache()

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
        if request.place_type:
            should_conds.append(QueryDSL.term(GeoPlaceIndexField.type, request.place_type.value))
            if request.place_type == GeoPlaceType.geoarea:
                # If we're looking for a general geoarea we exclude locality so that we are more
                # likely to find other areas first.
                must_not_conds.append(
                    QueryDSL.term(GeoPlaceIndexField.type, GeoPlaceType.locality.value)
                )

        should_conds = [
            *should_conds,
            *_continent_country_region_to_conditions(
                self._place_cache, request.in_continent, request.in_country, request.in_region
            ),
        ]

        # Dis_max is used so that the score will come from only the highest matching condition.
        name_match = QueryDSL.dis_max(
            QueryDSL.term(GeoPlaceIndexField.place_name_lower_keyword, request.name, boost=10.0),
            QueryDSL.term(
                GeoPlaceIndexField.alternate_names_lower_keyword, request.name, boost=5.0
            ),
            QueryDSL.match(GeoPlaceIndexField.place_name, request.name, fuzzy=True, boost=2.0),
            QueryDSL.match(GeoPlaceIndexField.alternate_names, request.name, fuzzy=True, boost=1.0),
        )
        must_conds.append(name_match)

        return SearchRequest(
            size=limit,
            query=QueryDSL.bool_cond(
                must_conds=must_conds, should_conds=should_conds, must_not_conds=must_not_conds
            ),
            sort=[
                SortField(field="_score", order="desc"),
                _TYPE_SORT_COND,
                _SOURCE_TYPE_SORT_COND,
                SortField(field="population", order="desc"),
            ],
            explain=explain,
        )

    @timed_function(logger)
    def search_for_places_raw(
        self,
        request: PlaceSearchRequest,
        *,
        limit: int = 5,
        explain: bool = False,
    ) -> SearchResponse:
        """TODO docs."""
        search_request = self.create_search_request(request, limit=limit, explain=explain)
        return self._index.search(search_request)

    def search(
        self,
        request: PlaceSearchRequest,
    ) -> BaseGeometry:
        """TODO docs."""
        search_resp = self.search_for_places_raw(request)
        places = search_resp.places
        if len(places) > 0:
            return places[0].geom
        # TODO I'm not sure this is the error that should be shown to the user.
        raise GeocodeError(
            f"Unable find place with name [{request.name}] "
            f"type [{request.place_type}] "
            f"in_continent [{request.in_continent}] "
            f"in_country [{request.in_country}] "
            f"in_region [{request.in_region}] "
        )


## Code for testing
# ruff: noqa: ERA001,E501,RUF100

# TODO mediterranean is still not sorting correctly

# from natural_language_geocoding.geocode_index.index import (  # noqa: E402
#     diff_explanations,
#     print_places_with_names,
# )

# lookup = GeocodeIndexPlaceLookup()

# resp = lookup.search_for_places_raw(
#     PlaceSearchRequest(
#         name="Mediterranean Sea", place_type=GeoPlaceType.sea, in_continent="Europe"
#     ),
#     explain=True,
#     limit=10,
# )

# index = GeocodeIndex()

# places = resp.places
# print_places_with_names(index, resp.places)

# diff_explanations(resp, 0, 2)
