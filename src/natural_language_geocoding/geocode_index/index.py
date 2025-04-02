"""TODO document this module."""

import json
import logging
import subprocess
from abc import ABC, abstractmethod
from typing import Any, Literal, TypedDict, cast

from e84_geoai_common.geometry import geometry_from_geojson_dict
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
    }
}


class GeoPlaceIndexField(IndexField):
    id = "id"
    place_name = "place_name"
    place_name_keyword = ("place_name", "keyword")
    type = "type"
    geom = "geom"
    source_id = "source_id"
    source_type = "source_type"
    source_path = "source_path"
    alternate_names = "alternate_names"
    alternate_names_keyword = ("alternate_names", "keyword")
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
            "fields": {GeoPlaceIndexField.place_name_keyword.name: {"type": "keyword"}},
        },
        GeoPlaceIndexField.type.name: {"type": "keyword"},
        GeoPlaceIndexField.geom.name: {"type": "geo_shape"},
        GeoPlaceIndexField.source_id.name: {"type": "long"},
        GeoPlaceIndexField.source_type.name: {"type": "keyword"},
        GeoPlaceIndexField.source_path.name: {"type": "keyword"},
        GeoPlaceIndexField.alternate_names.name: {
            "type": "text",
            "fields": {GeoPlaceIndexField.alternate_names_keyword.name: {"type": "keyword"}},
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
    place_name: str
    type: str
    geom: dict[str, Any]
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
        "place_name": geoplace.place_name,
        "type": geoplace.type.value,
        "geom": geoplace.geom.__geo_interface__,
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
        place_name=doc["place_name"],
        type=GeoPlaceType(doc["type"]),
        geom=geometry_from_geojson_dict(doc["geom"]),
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
    explain: bool = Field(
        description=(
            "If set to true opensearch will explain why each item appears in a particular order"
        ),
        default=False,
    )

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


class GeocodeIndexBase(ABC):
    @abstractmethod
    def create_index(self, *, recreate: bool = False) -> None: ...

    @abstractmethod
    def bulk_index(self, places: list[GeoPlace]) -> None: ...

    @abstractmethod
    def search(self, request: SearchRequest) -> SearchResponse: ...


class GeocodeIndex(GeocodeIndexBase):
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
            failed_items = [item["index"] for item in resp["items"] if "error" in item["index"]]
            self.logger.error("Failed ingesting items: %s", json.dumps(failed_items, indent=2))
            raise Exception("There were errors in the bulk index. See log")

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
                "name": place.place_name,
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
