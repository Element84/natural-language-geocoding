"""TODO document this module."""

import logging

from e84_geoai_common.util import timed_function
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.errors import GeocodeError
from natural_language_geocoding.geocode_index.geoplace import (
    PLACE_TYPE_SORT_ORDER,
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
from natural_language_geocoding.geocode_index.opensearch_utils import QueryCondition, QueryDSL
from natural_language_geocoding.place_lookup import PlaceLookup

type_order_values = [f"    '{pt.value}': {index}" for index, pt in enumerate(PLACE_TYPE_SORT_ORDER)]
type_order_values_str = "\n,".join(type_order_values)

# Note that using a script for sorting is slow. Eventually we should switch this to an indexed id
# to improve performance.
_TYPE_SORT_COND_SCRIPT = f"""
def typeOrder = [
    {type_order_values_str}
];
return typeOrder.containsKey(doc['type'].value) ? typeOrder[doc['type'].value] : 999;
""".strip()

_TYPE_SORT_COND = {
    "_script": {
        "type": "number",
        "script": {
            "source": _TYPE_SORT_COND_SCRIPT,
            "lang": "painless",
        },
        "order": "asc",
    }
}


class GeocodeIndexPlaceLookup(PlaceLookup):
    """TODO docs."""

    logger = logging.getLogger(f"{__name__}.{__qualname__}")

    _index: GeocodeIndex
    _place_cache: PlaceCache

    def __init__(self) -> None:
        self._index = GeocodeIndex()
        self._place_cache = PlaceCache()

    @timed_function(logger)
    def search_for_places_raw(  # noqa: PLR0913
        self,
        *,
        name: str,
        place_type: GeoPlaceType | None = None,
        continent_name: str | None = None,
        country_name: str | None = None,
        region_name: str | None = None,
        limit: int = 5,
        explain: bool = False,
    ) -> SearchResponse:
        """TODO docs."""
        # Dis_max is used so that the score will come from only the highest matching condition.

        should_conds: list[QueryCondition] = []
        if place_type:
            should_conds.append(QueryDSL.term(GeoPlaceIndexField.type, place_type.value))

        continent_ids: list[str] | None = None
        country_ids: list[str] | None = None

        if continent_name:
            continent_ids = self._place_cache.find_ids(
                name=continent_name, place_type=GeoPlaceType.continent
            )
            if len(continent_ids) == 0:
                raise ValueError(f"Unable to find continent with name [{continent_name}]")
            if len(continent_ids) > 1:
                raise Exception(
                    f"Unexpectedly found multiple continents with name [{continent_name}]"
                )
            should_conds.append(
                QueryDSL.term(GeoPlaceIndexField.hierarchies_continent_id, continent_ids[0])
            )

        if country_name:
            country_ids = self._place_cache.find_ids(
                name=country_name, place_type=GeoPlaceType.country, continent_ids=continent_ids
            )
            if len(country_ids) == 0:
                raise ValueError(f"Unable to find country with name [{country_name}]")
            should_conds.append(
                QueryDSL.terms(GeoPlaceIndexField.hierarchies_country_id, country_ids)
            )

        if region_name:
            region_ids = self._place_cache.find_ids(
                name=region_name,
                place_type=GeoPlaceType.region,
                continent_ids=continent_ids,
                country_ids=country_ids,
            )

            if len(region_ids) == 0:
                raise ValueError(f"Unable to find region with name [{region_name}]")
            should_conds.append(
                QueryDSL.terms(GeoPlaceIndexField.hierarchies_region_id, region_ids)
            )

        name_match = QueryDSL.dis_max(
            QueryDSL.term(GeoPlaceIndexField.place_name_keyword, name, boost=10.0),
            QueryDSL.term(GeoPlaceIndexField.alternate_names_keyword, name, boost=5.0),
            QueryDSL.match(GeoPlaceIndexField.place_name, name, fuzzy=True, boost=2.0),
            QueryDSL.match(GeoPlaceIndexField.alternate_names, name, fuzzy=True, boost=1.0),
        )

        request = SearchRequest(
            size=limit,
            query=QueryDSL.bool_cond(must_conds=[name_match], should_conds=should_conds),
            sort=[
                SortField(field="_score", order="desc"),
                _TYPE_SORT_COND,
                SortField(field="population", order="desc"),
            ],
            explain=explain,
        )
        return self._index.search(request)

    def search(
        self,
        *,
        name: str,
        place_type: GeoPlaceType | None = None,
        in_continent: str | None = None,
        in_country: str | None = None,
        in_region: str | None = None,
    ) -> BaseGeometry:
        """TODO docs."""
        search_resp = self.search_for_places_raw(
            name=name,
            place_type=place_type,
            continent_name=in_continent,
            country_name=in_country,
            region_name=in_region,
        )
        places = search_resp.places
        if len(places) > 0:
            return places[0].geom
        # TODO I'm not sure this is the error that should be shown to the user.
        raise GeocodeError(
            f"Unable find place with name [{name}] "
            f"type [{place_type}] "
            f"in_continent [{in_continent}] "
            f"in_country [{in_country}] "
            f"in_region [{in_region}] "
        )


## Code for testing
# ruff: noqa: ERA001,E501

# lookup = GeocodeIndexPlaceLookup()

# resp = lookup.search_for_places_raw(
#     name="Panama Canal", place_type=GeoPlaceType.channel, continent_name="North America"
# )

# places = resp.places
# print_places_as_table(resp.places)

# diff_explanations(resp, 0, 20)

# print(json.dumps(resp.body, indent=2))

# display_geometry([places[0].geom])
# display_geometry([places[1].geom])
# display_geometry([places[2].geom])
# display_geometry([places[3].geom])
# display_geometry([places[4].geom])
