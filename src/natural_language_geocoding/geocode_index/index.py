import json
import logging
import subprocess
from collections.abc import Generator
from typing import Any, Literal, TypedDict, cast

import boto3
from e84_geoai_common.geometry import geometry_from_geojson, geometry_to_geojson
from e84_geoai_common.util import get_env_var, singleline, timed_function
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection
from pydantic import BaseModel, ConfigDict, Field
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.geocode_index.geoplace import (
    PLACE_TYPE_SORT_ORDER,
    GeoPlace,
    GeoPlaceSource,
    GeoPlaceSourceType,
    GeoPlaceType,
    Hierarchy,
)
from natural_language_geocoding.place_lookup import PlaceLookup

_GEOPLACE_INDEX_SETTINGS: dict[str, Any] = {
    "index": {
        "number_of_shards": int(get_env_var("GEOCODE_INDEX_NUM_SHARDS", "5")),
        "refresh_interval": "30s",
        "number_of_replicas": 0,
    }
}
_GEOPLACE_INDEX_MAPPINGS = {
    "dynamic": "strict",
    "properties": {
        "id": {"type": "keyword"},
        "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
        "type": {"type": "keyword"},
        # We may not need to search it as a geometry
        # "geom": {"type": "geo_shape"},
        "geom": {"type": "keyword", "doc_values": False, "index": False},
        "source_id": {"type": "long"},
        "source_type": {"type": "keyword"},
        "source_path": {"type": "keyword"},
        "alternate_names": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
        "population": {"type": "long"},
        "area_sq_km": {"type": "double"},
        "properties": {"type": "keyword", "doc_values": False, "index": False},
        "hierarchies": {
            "type": "object",
            "dynamic": "strict",
            "properties": {
                "borough_id": {"type": "keyword"},
                "continent_id": {"type": "keyword"},
                "country_id": {"type": "keyword"},
                "county_id": {"type": "keyword"},
                "dependency_id": {"type": "keyword"},
                "disputed_id": {"type": "keyword"},
                "empire_id": {"type": "keyword"},
                "localadmin_id": {"type": "keyword"},
                "locality_id": {"type": "keyword"},
                "macrocounty_id": {"type": "keyword"},
                "macrohood_id": {"type": "keyword"},
                "macroregion_id": {"type": "keyword"},
                "marinearea_id": {"type": "keyword"},
                "marketarea_id": {"type": "keyword"},
                "microhood_id": {"type": "keyword"},
                "neighbourhood_id": {"type": "keyword"},
                "ocean_id": {"type": "keyword"},
                "postalregion_id": {"type": "keyword"},
                "region_id": {"type": "keyword"},
            },
        },
    },
}


class HierarchyDoc(TypedDict):
    borough_id: str | None
    continent_id: str | None
    country_id: str | None
    county_id: str | None
    dependency_id: str | None
    disputed_id: str | None
    empire_id: str | None
    localadmin_id: str | None
    locality_id: str | None
    macrocounty_id: str | None
    macrohood_id: str | None
    macroregion_id: str | None
    marinearea_id: str | None
    marketarea_id: str | None
    microhood_id: str | None
    neighbourhood_id: str | None
    ocean_id: str | None
    postalregion_id: str | None
    region_id: str | None


class GeoPlaceDoc(TypedDict):
    id: str
    name: str
    type: str
    geom: str
    source_id: int
    source_type: str
    source_path: str
    alternate_names: list[str]
    population: int | None
    area_sq_km: float | None
    properties: str
    hierarchies: list[HierarchyDoc]


_GEOPLACE_INDEX_NAME = "geoplaces"


def _geo_place_to_doc(geoplace: GeoPlace) -> GeoPlaceDoc:
    return {
        "id": geoplace.id,
        "name": geoplace.name,
        "type": geoplace.type.value,
        "geom": geometry_to_geojson(geoplace.geom),
        "source_id": geoplace.source_id,
        "source_type": geoplace.source.source_type.value,
        "source_path": geoplace.source.source_path,
        "alternate_names": geoplace.alternate_names,
        "properties": json.dumps(geoplace.properties),
        "hierarchies": cast("list[HierarchyDoc]", [h.model_dump() for h in geoplace.hierarchies]),
        "area_sq_km": geoplace.area_sq_km,
        "population": geoplace.population,
    }


def _doc_to_geo_place(doc: GeoPlaceDoc) -> GeoPlace:
    return GeoPlace(
        id=doc["id"],
        name=doc["name"],
        type=GeoPlaceType(doc["type"]),
        geom=geometry_from_geojson(doc["geom"]),
        source_id=doc["source_id"],
        source=GeoPlaceSource(
            source_type=GeoPlaceSourceType(doc["source_type"]), source_path=doc["source_path"]
        ),
        alternate_names=doc["alternate_names"],
        properties=json.loads(doc["properties"]),
        hierarchies=[
            Hierarchy.model_validate(hierarchy) for hierarchy in doc.get("hierarchies", [])
        ],
        population=doc["population"],
        area_sq_km=doc["area_sq_km"],
    )


class SearchResponse(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    took_ms: int
    hits: int
    places: list[GeoPlace]
    body: dict[str, Any]
    explanations: list[dict[str, Any]] | None

    @staticmethod
    def from_search_resp(body: dict[str, Any]) -> "SearchResponse":
        explanations: list[dict[str, Any]] | None = None
        hits = body["hits"]["hits"]
        if len(hits) > 0 and "_explanation" in hits[0]:
            explanations = [h["_explanation"] for h in hits]

        return SearchResponse(
            body=body,
            took_ms=body["took"],
            hits=body["hits"]["total"]["value"],
            places=[_doc_to_geo_place(hit["_source"]) for hit in hits],
            explanations=explanations,
        )


class SortField(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    field: str
    order: Literal["asc", "desc"] = "asc"

    def to_opensearch(self) -> dict[str, Any]:
        return {self.field: {"order": self.order}}


class SearchRequest(BaseModel):
    model_config = ConfigDict(
        strict=True,
        extra="forbid",
        frozen=True,
    )
    search_type: Literal["query_then_fetch", "dfs_query_then_fetch"] = Field(
        default="dfs_query_then_fetch",
        description=singleline("""
            Whether OpenSearch should use global term and document frequencies when calculating
            relevance scores. dfs_query_then_fetch provides consistent scores calculated using the
            whole index but may be slightly slower.
        """),
    )

    size: int = 10
    start_index: int = 0
    sort: list[str | SortField | dict[str, Any]] | None = None
    query: dict[str, Any]
    explain: bool = False

    def to_opensearch_params(self) -> dict[str, Any]:
        return {
            "search_type": self.search_type,
            "explain": str(self.explain).lower(),
        }

    def to_opensearch_body(self) -> dict[str, Any]:
        def _to_sort_arg(sort_part: str | SortField | dict[str, Any]) -> str | dict[str, Any]:
            if isinstance(sort_part, str):
                return sort_part
            if isinstance(sort_part, dict):
                return sort_part
            return sort_part.to_opensearch()

        body: dict[str, Any] = {
            "query": self.query,
            "size": self.size,
            "from": self.start_index,
        }
        if self.sort:
            body["sort"] = [_to_sort_arg(sort_part) for sort_part in self.sort]
        return body


def _create_opensearch_client() -> OpenSearch:
    # TODO include these env vars as part of the documentation
    host = get_env_var("GEOCODE_INDEX_HOST")
    port = int(get_env_var("GEOCODE_INDEX_PORT", "443"))
    region = get_env_var("GEOCODE_INDEX_REGION")
    credentials = boto3.Session().get_credentials()
    auth = AWSV4SignerAuth(credentials, region, "es")

    if host == "localhost":
        # Allow tunneling for easy local testing.
        return OpenSearch(
            hosts=[{"host": host, "port": port}],
            use_ssl=True,
            verify_certs=False,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20,
        )
    return OpenSearch(
        hosts=[{"host": host, "port": port}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=host != "localhost",
        connection_class=RequestsHttpConnection,
        pool_maxsize=20,
    )


class GeocodeIndex:
    logger = logging.getLogger(f"{__name__}.{__qualname__}")
    client: OpenSearch

    def __init__(self) -> None:
        self.client = _create_opensearch_client()

    def create_index(self, *, recreate: bool = False) -> None:
        if recreate and self.client.indices.exists(index=_GEOPLACE_INDEX_NAME):
            self.logger.warning(
                "Deleting the existing index %s before creating it", _GEOPLACE_INDEX_NAME
            )
            self.client.indices.delete(index=_GEOPLACE_INDEX_NAME)

        self.logger.info("Creating index %s", _GEOPLACE_INDEX_NAME)
        self.client.indices.create(
            _GEOPLACE_INDEX_NAME,
            {
                "settings": _GEOPLACE_INDEX_SETTINGS,
                "mappings": _GEOPLACE_INDEX_MAPPINGS,
            },
        )

    def bulk_index(self, places: list[GeoPlace]) -> None:
        bulk_command_lines = [
            json.dumps(bulk_line)
            for place in places
            for bulk_line in [
                {"index": {"_index": _GEOPLACE_INDEX_NAME, "_id": place.id}},
                _geo_place_to_doc(place),
            ]
        ]
        bulk_body = "\n".join(bulk_command_lines)
        resp = self.client.bulk(bulk_body)

        if resp["errors"]:
            print(json.dumps(resp))  # noqa: T201
            raise Exception("There were errors in the bulk index")

    @timed_function(logger)
    def search(self, request: SearchRequest) -> SearchResponse:
        params = request.to_opensearch_params()
        body = request.to_opensearch_body()

        self.logger.info(
            "Searching for geoplaces with params %s and body %s", params, json.dumps(body)
        )
        resp = self.client.search(index=_GEOPLACE_INDEX_NAME, params=params, body=body)
        return SearchResponse.from_search_resp(resp)


QueryCondition = dict[str, Any]


class QueryDSL:
    @staticmethod
    def and_conds(*conds: QueryCondition) -> QueryCondition:
        return {"bool": {"must": conds}}

    @staticmethod
    def or_conds(*conds: QueryCondition) -> QueryCondition:
        return {"bool": {"should": conds}}

    @staticmethod
    def dis_max(*conds: QueryCondition) -> QueryCondition:
        """Combines conjunctions into a dis_max query.

        See https://opensearch.org/docs/latest/query-dsl/compound/disjunction-max/
        """
        return {"dis_max": {"queries": conds}}

    @staticmethod
    def match(
        field: str, text: str, *, fuzzy: bool = False, boost: float | None = None
    ) -> QueryCondition:
        inner_cond: dict[str, str | int | float] = {"query": text}
        if fuzzy:
            inner_cond["fuzziness"] = "AUTO"
        if boost is not None:
            inner_cond["boost"] = boost
        return {"match": {field: inner_cond}}

    @staticmethod
    def term(field: str, value: str, *, boost: float | None = None) -> QueryCondition:
        inner_cond: dict[str, str | float] = {"value": value}
        if boost is not None:
            inner_cond["boost"] = boost

        return {"term": {field: inner_cond}}

    @staticmethod
    def terms(field: str, values: list[str], *, boost: float | None = None) -> QueryCondition:
        if len(values) == 0:
            raise ValueError("Must have one or more values")
        inner_cond: dict[str, float | list[str]] = {field: values}

        if boost is not None:
            inner_cond["boost"] = boost

        return {"terms": inner_cond}


class Hit(TypedDict):
    _id: str
    _source: dict[str, Any]


def _scroll_fetch_all(
    client: OpenSearch,
    *,
    query: QueryCondition,
    source_fields: list[str],
) -> Generator[Hit, None, None]:
    body = {"query": query, "_source": source_fields, "size": 1000}

    # Initialize the scroll
    scroll_resp: dict[str, Any] = client.search(
        index=_GEOPLACE_INDEX_NAME, body=body, params={"scroll": "2m"}
    )

    hits = scroll_resp["hits"]["hits"]
    hits_count = len(hits)
    scroll_id = scroll_resp["_scroll_id"]

    # Continue scrolling until no more hits are returned
    while hits_count > 0:
        scroll_resp = client.scroll(scroll_id=scroll_id, params={"scroll": "2m"})
        hits = scroll_resp["hits"]["hits"]
        hits_count = len(hits)
        scroll_id = scroll_resp["_scroll_id"]
        yield from hits

    # Clear the scroll to free resources
    client.clear_scroll(scroll_id=scroll_id)


class HierarchicalPlaceCache:
    _id_to_name_place: dict[str, tuple[str, GeoPlaceType]]
    _name_place_to_id: dict[tuple[str, GeoPlaceType], str]
    _supported_place_types: set[GeoPlaceType]

    def __init__(self, supported_place_types: set[GeoPlaceType] | None = None) -> None:
        self._id_to_name_place = {}
        self._name_place_to_id = {}
        # Counts of placetypes during initial implementation
        # continent - 8
        # empire - 11
        # country - 219
        # region - 4865
        self._supported_place_types = supported_place_types or {
            GeoPlaceType.continent,
            GeoPlaceType.empire,
            GeoPlaceType.country,
            GeoPlaceType.region,
        }

    @timed_function
    def populate(self) -> None:
        client = _create_opensearch_client()

        self._id_to_name_place = {}
        self._name_place_to_id = {}

        for hit in _scroll_fetch_all(
            client,
            query=QueryDSL.terms("type", [p.value for p in self._supported_place_types]),
            source_fields=["name", "type"],
        ):
            feature_id = hit["_id"]
            place_type = GeoPlaceType(hit["_source"]["type"])
            name = hit["_source"]["name"]
            self._id_to_name_place[feature_id] = (name, place_type)
            self._name_place_to_id[(name, place_type)] = feature_id

    def find_id_by_name_and_type(self, name: str, place_type: GeoPlaceType) -> str | None:
        return self._name_place_to_id.get((name, place_type))

    def find_name_and_type_by_id(self, feature_id: str) -> tuple[str, GeoPlaceType] | None:
        return self._id_to_name_place.get(feature_id)


type_order_values = [f"    '{pt.value}': {index}" for index, pt in enumerate(PLACE_TYPE_SORT_ORDER)]
type_order_values_str = "\n,".join(type_order_values)

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
    logger = logging.getLogger(f"{__name__}.{__qualname__}")

    _index: GeocodeIndex
    _place_cache: HierarchicalPlaceCache

    def __init__(self) -> None:
        self._index = GeocodeIndex()
        self._place_cache = HierarchicalPlaceCache()
        self._place_cache.populate()

    @timed_function(logger)
    def search_for_places_raw(  # noqa: PLR0913
        self,
        *,
        name: str,
        place_type: GeoPlaceType | None = None,
        in_continent: str | None = None,
        in_country: str | None = None,
        in_region: str | None = None,
        limit: int = 5,
        explain: bool = False,
    ) -> SearchResponse:
        # Dis_max is used so that the score will come from only the highest matching condition.
        name_match = QueryDSL.dis_max(
            QueryDSL.term("name.keyword", name, boost=10.0),
            QueryDSL.term("alternate_names.keyword", name, boost=5.0),
            QueryDSL.match("name", name, fuzzy=True, boost=2.0),
            QueryDSL.match("alternate_names", name, fuzzy=True, boost=1.0),
        )
        conditions: list[QueryCondition] = [name_match]
        if place_type:
            conditions.append(QueryDSL.term("type", place_type.value))

        within_conds: list[QueryCondition] = []

        if in_continent:
            continent_id = self._place_cache.find_id_by_name_and_type(
                in_continent, GeoPlaceType.continent
            )
            if continent_id is None:
                raise ValueError(f"Unable to find continent with name [{in_continent}]")
            within_conds.append(QueryDSL.term("hierarchies.continent_id", continent_id))
        if in_country:
            country_id = self._place_cache.find_id_by_name_and_type(
                in_country, GeoPlaceType.country
            )
            if country_id is None:
                raise ValueError(f"Unable to find country with name [{in_country}]")
            within_conds.append(QueryDSL.term("hierarchies.country_id", country_id))
        if in_region:
            region_id = self._place_cache.find_id_by_name_and_type(in_region, GeoPlaceType.region)
            if region_id is None:
                raise ValueError(f"Unable to find region with name [{in_region}]")
            within_conds.append(QueryDSL.term("hierarchies.region_id", region_id))

        request = SearchRequest(
            size=limit,
            query=QueryDSL.and_conds(*conditions, *within_conds),
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
        search_resp = self.search_for_places_raw(
            name=name,
            place_type=place_type,
            in_continent=in_continent,
            in_country=in_country,
            in_region=in_region,
        )
        places = search_resp.places
        if len(places) > 0:
            return places[0].geom
        raise Exception(
            f"Unable find place with name [{name}] "
            f"type [{place_type}] "
            f"in_continent [{in_continent}] "
            f"in_country [{in_country}] "
            f"in_region [{in_region}] "
        )


def diff_explanations(resp: SearchResponse, index1: int, index2: int) -> None:
    """A utility for explaining the differences between search result order.

    Explanations must be enabled on the search results. Writes the explanations to local files and
    then opens the files in vscode.
    """
    if resp.explanations is None:
        raise Exception("explanations are not present")
    place1 = resp.places[index1]
    place2 = resp.places[index2]
    exp1 = resp.explanations[index1]
    exp2 = resp.explanations[index2]

    def to_compare_str(place: GeoPlace, exp: dict[str, Any]) -> str:
        return json.dumps(
            {
                "name": place.name,
                "type": place.type.value,
                "alternate_names": place.alternate_names,
                "explanation": exp,
            },
            indent=2,
        )

    with open("temp/compare1.json", "w") as f:  # noqa: PTH123
        f.write(to_compare_str(place1, exp1))
    with open("temp/compare2.json", "w") as f:  # noqa: PTH123
        f.write(to_compare_str(place2, exp2))

    subprocess.run(["code", "temp/compare1.json"], check=True)  # noqa: S603, S607
    subprocess.run(["code", "temp/compare2.json"], check=True)  # noqa: S603, S607


## Code for testing
# ruff: noqa: ERA001


# lookup = GeocodeIndexPlaceLookup()

# resp = lookup.search_for_places_raw(name="Florida", limit=30, explain=True)
# places = resp.places
# print_places_as_table(resp.places)

# diff_explanations(resp, 0, 20)

# print(json.dumps(resp.body, indent=2))

# display_geometry([places[0].geom])
# display_geometry([places[1].geom])
# display_geometry([places[2].geom])
# display_geometry([places[3].geom])
# display_geometry([places[4].geom])
