"""Provides types for creating, ingest, and searching geospatial places in an opensearch index."""

import json
import logging
import subprocess
from abc import ABC, abstractmethod
from collections.abc import Iterable
from time import time
from typing import Any, Literal, TypedDict, cast

from e84_geoai_common.geometry import geometry_from_geojson
from e84_geoai_common.util import get_env_var, singleline, timed_function
from opensearchpy import OpenSearch
from pydantic import BaseModel, ConfigDict, Field

from natural_language_geocoding.geocode_index.geoplace import (
    GeoPlace,
    GeoPlaceSource,
    GeoPlaceSourceType,
    GeoPlaceType,
    Hierarchy,
)
from natural_language_geocoding.geocode_index.opensearch_utils import (
    IndexField,
    create_opensearch_client,
)

_GEOPLACE_INDEX_SETTINGS: dict[str, Any] = {
    "index": {
        "number_of_shards": int(get_env_var("GEOCODE_INDEX_NUM_SHARDS", "5")),
        "refresh_interval": "30s",
        "number_of_replicas": 0,
    },
    "analysis": {"normalizer": {"lowercase": {"type": "custom", "filter": ["lowercase"]}}},
}


class GeoPlaceIndexField(IndexField):
    """Defines the different index fields on the GeoPlace index."""

    id = "id"
    place_name = "place_name"
    place_name_keyword = ("place_name", "keyword")
    place_name_lower_keyword = ("place_name", "lowercase")
    type = "type"
    geom_str = "geom_str"
    geom_spatial = "geom_spatial"
    source_id = "source_id"
    source_type = "source_type"
    source_path = "source_path"
    alternate_names = "alternate_names"
    alternate_names_keyword = ("alternate_names", "keyword")
    alternate_names_lower_keyword = ("alternate_names", "lowercase")
    population = "population"
    area_sq_km = "area_sq_km"
    properties = "properties"
    hierarchies = "hierarchies"

    hierarchies_borough_id = ("hierarchies", "borough_id")
    hierarchies_continent_id = ("hierarchies", "continent_id")
    hierarchies_country_id = ("hierarchies", "country_id")
    hierarchies_county_id = ("hierarchies", "county_id")
    hierarchies_dependency_id = ("hierarchies", "dependency_id")
    hierarchies_disputed_id = ("hierarchies", "disputed_id")
    hierarchies_empire_id = ("hierarchies", "empire_id")
    hierarchies_localadmin_id = ("hierarchies", "localadmin_id")
    hierarchies_locality_id = ("hierarchies", "locality_id")
    hierarchies_macrocounty_id = ("hierarchies", "macrocounty_id")
    hierarchies_macrohood_id = ("hierarchies", "macrohood_id")
    hierarchies_macroregion_id = ("hierarchies", "macroregion_id")
    hierarchies_marinearea_id = ("hierarchies", "marinearea_id")
    hierarchies_marketarea_id = ("hierarchies", "marketarea_id")
    hierarchies_microhood_id = ("hierarchies", "microhood_id")
    hierarchies_neighbourhood_id = ("hierarchies", "neighbourhood_id")
    hierarchies_ocean_id = ("hierarchies", "ocean_id")
    hierarchies_postalregion_id = ("hierarchies", "postalregion_id")
    hierarchies_region_id = ("hierarchies", "region_id")


_GEOPLACE_INDEX_MAPPINGS = {
    "dynamic": "strict",
    "properties": {
        GeoPlaceIndexField.id.name: {"type": "keyword"},
        GeoPlaceIndexField.place_name.name: {
            "type": "text",
            "fields": {
                GeoPlaceIndexField.place_name_keyword.name: {"type": "keyword"},
                GeoPlaceIndexField.place_name_lower_keyword.name: {
                    "type": "keyword",
                    "normalizer": "lowercase",
                },
            },
        },
        GeoPlaceIndexField.type.name: {"type": "keyword"},
        # The geometry of the place as an indexed geo shape
        GeoPlaceIndexField.geom_spatial.name: {"type": "geo_shape"},
        # The geometry of the place as a JSON string.
        GeoPlaceIndexField.geom_str.name: {"type": "keyword", "doc_values": False, "index": False},
        GeoPlaceIndexField.source_id.name: {"type": "long"},
        GeoPlaceIndexField.source_type.name: {"type": "keyword"},
        GeoPlaceIndexField.source_path.name: {"type": "keyword"},
        GeoPlaceIndexField.alternate_names.name: {
            "type": "text",
            "fields": {
                GeoPlaceIndexField.alternate_names_keyword.name: {"type": "keyword"},
                GeoPlaceIndexField.alternate_names_lower_keyword.name: {
                    "type": "keyword",
                    "normalizer": "lowercase",
                },
            },
        },
        GeoPlaceIndexField.population.name: {"type": "long"},
        GeoPlaceIndexField.area_sq_km.name: {"type": "double"},
        GeoPlaceIndexField.properties.name: {
            "type": "keyword",
            "doc_values": False,
            "index": False,
        },
        GeoPlaceIndexField.hierarchies.name: {
            "type": "object",
            "dynamic": "strict",
            "properties": {
                GeoPlaceIndexField.hierarchies_borough_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_continent_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_country_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_county_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_dependency_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_disputed_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_empire_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_localadmin_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_locality_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_macrocounty_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_macrohood_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_macroregion_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_marinearea_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_marketarea_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_microhood_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_neighbourhood_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_ocean_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_postalregion_id.name: {"type": "keyword"},
                GeoPlaceIndexField.hierarchies_region_id.name: {"type": "keyword"},
            },
        },
    },
}


class HierarchyDoc(TypedDict):
    """Identifies the ids of parent places for a geoplace."""

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
    """Represents an indexed geoplace."""

    id: str
    place_name: str
    type: str
    geom_spatial: dict[str, Any] | None
    geom_str: str
    source_type: str
    source_path: str
    alternate_names: list[str]
    population: int | None
    area_sq_km: float | None
    properties: str
    hierarchies: list[HierarchyDoc]


GEOPLACE_INDEX_NAME = "geoplaces"

# The set of geo place types for which we'll index geometry spatially.
# We don't do this for all types due to some issues getting everything to index. In the future, we
# may index more.
_SPATIAL_INDEXED_TYPES = {GeoPlaceType.continent, GeoPlaceType.country, GeoPlaceType.region}


def _geo_place_to_doc(geoplace: GeoPlace) -> GeoPlaceDoc:
    """Converts a GeoPlace model into an opensearch document for indexing."""
    doc: GeoPlaceDoc = {
        "id": geoplace.id,
        "place_name": geoplace.place_name,
        "type": geoplace.type_value,
        "geom_str": json.dumps(geoplace.geom.__geo_interface__),
        "geom_spatial": None,
        "source_type": geoplace.source.source_type_value,
        "source_path": geoplace.source.source_path,
        "alternate_names": geoplace.alternate_names,
        "properties": json.dumps(geoplace.properties),
        "hierarchies": cast("list[HierarchyDoc]", [h.model_dump() for h in geoplace.hierarchies]),
        "area_sq_km": geoplace.area_sq_km,
        "population": geoplace.population,
    }
    if geoplace.type in _SPATIAL_INDEXED_TYPES:
        doc["geom_spatial"] = geoplace.geom.__geo_interface__
    return doc


class FoundGeoPlace(GeoPlace):
    """A Geoplace that has been found in a search query."""

    score: float | None
    sort: list[float] | None

    @staticmethod
    def from_hit(hit: dict[str, Any]) -> "FoundGeoPlace":
        doc: GeoPlaceDoc = hit["_source"]
        """Converts an opensearch document to the GeoPlace model."""
        place_type = GeoPlaceType(doc["type"]) if doc["type"] in GeoPlaceType else doc["type"]
        source_type = (
            GeoPlaceSourceType(doc["source_type"])
            if doc["source_type"] in GeoPlaceSourceType
            else doc["source_type"]
        )

        return FoundGeoPlace(
            id=doc["id"],
            place_name=doc["place_name"],
            type=place_type,
            geom=geometry_from_geojson(doc["geom_str"]),
            source=GeoPlaceSource(source_type=source_type, source_path=doc["source_path"]),
            alternate_names=doc["alternate_names"],
            properties=json.loads(doc["properties"]),
            hierarchies=[
                Hierarchy.model_validate(hierarchy) for hierarchy in doc.get("hierarchies", [])
            ],
            population=doc["population"],
            area_sq_km=doc["area_sq_km"],
            score=hit.get("_score"),
            sort=hit.get("sort"),
        )


class SearchResponse(BaseModel):
    """Contains results of a search for geoplaces."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    took_ms: int
    hits: int
    places: list[FoundGeoPlace]
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
            places=[FoundGeoPlace.from_hit(hit) for hit in hits],
            explanations=explanations,
        )


class SortField(BaseModel):
    """Represents a field for sorting and a direction."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    field: str
    order: Literal["asc", "desc"] = "asc"

    def to_opensearch(self) -> dict[str, Any]:
        return {self.field: {"order": self.order}}


class SearchRequest(BaseModel):
    """A search request for finding geoplaces."""

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

    size: int = Field(description="The number of results to return", default=10)

    start_index: int = 0

    sort: list[str | SortField | dict[str, Any]] | None = None

    query: dict[str, Any]

    explain: bool = Field(
        description=(
            "If set to true opensearch will explain why each item appears in a particular order"
        ),
        default=False,
    )

    def to_opensearch_params(self) -> dict[str, Any]:
        """Returns the opensearch query parameters for this search.."""
        return {
            "search_type": self.search_type,
            "explain": str(self.explain).lower(),
        }

    def to_opensearch_body(self) -> dict[str, Any]:
        """Converts this to an opensearch search request body."""

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


class GeocodeIndexBase(ABC):
    """Abstract base class for geospatial place indexing and search operations.

    This class defines the interface for creating, populating, and searching an index
    of geospatial places. Implementations of this class handle the specific details
    of interacting with the underlying search engine (such as OpenSearch).
    """

    @abstractmethod
    def create_index(self, *, recreate: bool = False) -> None:
        """Create the geospatial place index.

        This method initializes the index with the appropriate schema and settings.
        If the index already exists, the behavior depends on the recreate parameter.

        Args:
            recreate: If True, delete any existing index before creating a new one.
                     If False, keep the existing index if it exists.
        """
        ...

    @abstractmethod
    def bulk_index(self, places: list[GeoPlace]) -> None:
        """Index multiple geospatial places in a single operation.

        This method adds or updates a batch of places in the index.

        Args:
            places: List of GeoPlace objects to index.

        Raises:
            Exception: If the indexing operation fails.
        """
        ...

    @abstractmethod
    def search(self, request: SearchRequest) -> SearchResponse:
        """Search for geospatial places matching the given criteria.

        This method executes a search against the index using the parameters
        specified in the request.

        Args:
            request: A SearchRequest object containing the search parameters.

        Returns:
            A SearchResponse object containing the search results.
        """
        ...

    @abstractmethod
    def get_by_ids(self, ids: Iterable[str]) -> list[GeoPlace]:
        """Fetches geoplaces by id."""
        ...


class GeocodeIndex(GeocodeIndexBase):
    """Implementation of GeocodeIndexBase against opensearch cluster.

    See base class for documentation.
    """

    logger = logging.getLogger(f"{__name__}.{__qualname__}")
    client: OpenSearch

    def __init__(self, client: OpenSearch | None = None) -> None:
        self.client = client or create_opensearch_client()

    def create_index(self, *, recreate: bool = False) -> None:
        if recreate and self.client.indices.exists(index=GEOPLACE_INDEX_NAME):
            self.logger.warning(
                "Deleting the existing index %s before creating it", GEOPLACE_INDEX_NAME
            )
            self.client.indices.delete(index=GEOPLACE_INDEX_NAME)

        self.logger.info("Creating index %s", GEOPLACE_INDEX_NAME)
        self.client.indices.create(
            index=GEOPLACE_INDEX_NAME,
            body={
                "settings": _GEOPLACE_INDEX_SETTINGS,
                "mappings": _GEOPLACE_INDEX_MAPPINGS,
            },
        )

    def bulk_index(self, places: list[GeoPlace]) -> None:
        bulk_command_lines = [
            json.dumps(bulk_line)
            for place in places
            for bulk_line in [
                {"index": {"_index": GEOPLACE_INDEX_NAME, "_id": place.id}},
                _geo_place_to_doc(place),
            ]
        ]
        bulk_body = "\n".join(bulk_command_lines)
        resp = self.client.bulk(body=bulk_body)

        if resp["errors"]:
            failed_items = [item["index"] for item in resp["items"] if "error" in item["index"]]
            self.logger.error("Failed ingesting items: %s", json.dumps(failed_items, indent=2))
            raise Exception("There were errors in the bulk index. See log")

    def search(self, request: SearchRequest) -> SearchResponse:
        params = request.to_opensearch_params()
        body = request.to_opensearch_body()

        self.logger.info(
            "Searching for geoplaces with params %s and body %s", params, json.dumps(body)
        )
        start = time()
        os_resp = self.client.search(index=GEOPLACE_INDEX_NAME, params=params, body=body)
        duration = time() - start
        resp = SearchResponse.from_search_resp(os_resp)
        self.logger.info(
            "Searching for geoplaces took %s seconds with opensearch reporting %s seconds",
            duration,
            resp.took_ms / 1000,
        )
        return resp

    @timed_function(logger)
    def get_by_ids(self, ids: Iterable[str]) -> list[GeoPlace]:
        resp = self.client.mget(
            body={"docs": [{"_id": place_id} for place_id in ids]}, index=GEOPLACE_INDEX_NAME
        )
        return [FoundGeoPlace.from_hit(doc) for doc in resp["docs"]]

    @timed_function(logger)
    def get_names_by_ids(self, ids: Iterable[str]) -> dict[str, str]:
        resp = self.client.mget(
            body={
                "docs": [
                    {"_id": place_id, "_source": {"include": GeoPlaceIndexField.place_name.value}}
                    for place_id in ids
                ]
            },
            index=GEOPLACE_INDEX_NAME,
        )
        return {
            doc["_id"]: doc["_source"][GeoPlaceIndexField.place_name.value]
            for doc in resp["docs"]
            if doc["found"]
        }


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
                "place_name": place.place_name,
                "type": place.type_value,
                "alternate_names": place.alternate_names,
                "explanation": exp,
            },
            indent=2,
        )

    with open("temp/compare1.json", "w") as f:  # noqa: PTH123
        f.write(to_compare_str(place1, exp1))
    with open("temp/compare2.json", "w") as f:  # noqa: PTH123
        f.write(to_compare_str(place2, exp2))

    subprocess.run(["code", "temp/compare1.json"], check=True)  # noqa: S607
    subprocess.run(["code", "temp/compare2.json"], check=True)  # noqa: S607


def print_hierarchies_with_names(index: GeocodeIndex, hierarchies: list[Hierarchy]) -> None:
    """Prints hierarchies as a table. Useful for debugging."""
    places = index.get_by_ids(
        [place_id for h in hierarchies for place_id in h.model_dump(exclude_none=True).values()]
    )
    id_to_name = {p.id: p.place_name for p in places}

    table_data: list[dict[str, Any]] = [
        {field: id_to_name[place_id] for field, place_id in h.model_dump(exclude_none=True).items()}
        for h in hierarchies
    ]

    from tabulate import tabulate  # noqa: PLC0415

    # Print the table
    print(tabulate(table_data, headers="keys", tablefmt="grid"))  # noqa: T201


def print_hierarchies_as_table(hierarchies: list[Hierarchy]) -> None:
    """Prints hierarchies as a table. Useful for debugging."""
    table_data: list[dict[str, Any]] = [h.model_dump(exclude_none=True) for h in hierarchies]

    # Print the table
    from tabulate import tabulate  # noqa: PLC0415

    print(tabulate(table_data, headers="keys", tablefmt="grid"))  # noqa: T201


def print_places_with_names(index: GeocodeIndex, places: list[FoundGeoPlace]) -> None:
    """Prints places as a table with hierarchy names. Useful for debugging."""
    all_ids = {
        place_id
        for place in places
        for h in place.hierarchies
        for place_id in h.model_dump(exclude_none=True).values()
    }

    id_to_name = index.get_names_by_ids(all_ids)

    table_data: list[dict[str, Any]] = []
    for idx, place in enumerate(places):
        place_dict = {
            "index": idx,
            "score": place.score,
            "sort": place.sort,
            "id": place.id,
            "name": place.place_name,
            "type": place.type_value,
            "alternate_names": place.alternate_names,
            "hierarchies": [
                {k: id_to_name.get(v, v) for k, v in h if v is not None} for h in place.hierarchies
            ],
        }
        table_data.append(place_dict)

    from tabulate import tabulate  # noqa: PLC0415

    # Print the table
    print(tabulate(table_data, headers="keys", tablefmt="grid"))  # noqa: T201


##################
# Code for manual testing
# ruff: noqa: ERA001


# index = GeocodeIndex()


# query = {
#     "bool": {
#         "should": [
#             {"term": {"type": {"value": "desert"}}},
#             {"term": {"hierarchies.continent_id": {"value": "wof_102191573"}}},
#         ],
#         "must": [
#             {
#                 "dis_max": {
#                     "queries": [
#                         {"term": {"place_name.keyword": {"value": "Sahara", "boost": 10.0}}},
#                         {"term": {"alternate_names.keyword": {"value": "Sahara", "boost": 5.0}}},
#                         {
#                             "match": {
#                                 "place_name": {
#                                   "query": "Sahara", "fuzziness": "AUTO", "boost": 2.0}
#                             }
#                         },
#                         {
#                             "match": {
#                                 "alternate_names": {
#                                     "query": "Sahara",
#                                     "fuzziness": "AUTO",
#                                     "boost": 1.0,
#                                 }
#                             }
#                         },
#                     ]
#                 }
#             }
#         ],
#     }
# }

# resp = index.search(SearchRequest(query=query, size=30, explain=True))


# print_places_with_names(index, resp.places)

# diff_explanations(resp, 25, 26)
