import json
import logging
import subprocess
from typing import Any, Literal, TypedDict, cast

from e84_geoai_common.geometry import geometry_from_geojson, geometry_to_geojson
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
    create_opensearch_client,
)

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
        # "geom": {"type": "geo_shape"},  # noqa: ERA001
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


GEOPLACE_INDEX_NAME = "geoplaces"


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


class GeocodeIndex:
    logger = logging.getLogger(f"{__name__}.{__qualname__}")
    client: OpenSearch

    def __init__(self) -> None:
        self.client = create_opensearch_client()

    def create_index(self, *, recreate: bool = False) -> None:
        if recreate and self.client.indices.exists(index=GEOPLACE_INDEX_NAME):
            self.logger.warning(
                "Deleting the existing index %s before creating it", GEOPLACE_INDEX_NAME
            )
            self.client.indices.delete(index=GEOPLACE_INDEX_NAME)

        self.logger.info("Creating index %s", GEOPLACE_INDEX_NAME)
        self.client.indices.create(
            GEOPLACE_INDEX_NAME,
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
                {"index": {"_index": GEOPLACE_INDEX_NAME, "_id": place.id}},
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
        resp = self.client.search(index=GEOPLACE_INDEX_NAME, params=params, body=body)
        return SearchResponse.from_search_resp(resp)


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
