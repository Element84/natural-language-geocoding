"""Provides types for creating, ingesting, and searching lunar places in an OpenSearch index."""

import json
import logging
from collections.abc import Iterable
from enum import Enum
from time import time
from typing import Any, TypedDict, cast

from e84_geoai_common.geometry import geometry_from_geojson
from e84_geoai_common.util import get_env_var, timed_function
from opensearchpy import OpenSearch

from natural_language_geocoding.geocode_index.geoplace import (
    GeoPlace,
    GeoPlaceSource,
)
from natural_language_geocoding.geocode_index.index import (
    FoundGeoPlace,
    GeocodeIndexBase,
    SearchRequest,
    SearchResponse,
)
from natural_language_geocoding.geocode_index.lunar_place_types import LunarPlaceType
from natural_language_geocoding.geocode_index.opensearch_utils import (
    IndexField,
    create_opensearch_client,
)

logger = logging.getLogger(__name__)


class LunarPlaceSourceType(Enum):
    """Source types for lunar place data."""

    # USGS Planetary Nomenclature
    usgs_planetary = "usgs_planetary"


class LunarPlaceSource(GeoPlaceSource):
    """Identifies the source of a lunar place."""

    source_type: LunarPlaceSourceType | str

    @property
    def source_type_value(self) -> str:
        """Returns the source type as a string."""
        return self.source_type if isinstance(self.source_type, str) else self.source_type.value


_LUNAR_INDEX_SETTINGS: dict[str, Any] = {
    "index": {
        "number_of_shards": int(get_env_var("GEOCODE_INDEX_NUM_SHARDS", "2")),
        "refresh_interval": "30s",
        "number_of_replicas": 0,
    },
    "analysis": {"normalizer": {"lowercase": {"type": "custom", "filter": ["lowercase"]}}},
}


class LunarPlaceIndexField(IndexField):
    """Defines the different index fields on the lunar GeoPlace index."""

    id = "id"
    place_name = "place_name"
    place_name_keyword = ("place_name", "keyword")
    place_name_lower_keyword = ("place_name", "lowercase")
    type = "type"
    type_code = "type_code"
    geom_str = "geom_str"
    geom_spatial = "geom_spatial"
    source_type = "source_type"
    source_path = "source_path"
    alternate_names = "alternate_names"
    alternate_names_keyword = ("alternate_names", "keyword")
    alternate_names_lower_keyword = ("alternate_names", "lowercase")
    area_sq_km = "area_sq_km"
    diameter_km = "diameter_km"
    center_lat = "center_lat"
    center_lon = "center_lon"
    quad_name = "quad_name"
    origin = "origin"
    properties = "properties"


_LUNAR_GEOPLACE_INDEX_MAPPINGS = {
    "dynamic": "strict",
    "properties": {
        LunarPlaceIndexField.id.name: {"type": "keyword"},
        LunarPlaceIndexField.place_name.name: {
            "type": "text",
            "fields": {
                LunarPlaceIndexField.place_name_keyword.name: {"type": "keyword"},
                LunarPlaceIndexField.place_name_lower_keyword.name: {
                    "type": "keyword",
                    "normalizer": "lowercase",
                },
            },
        },
        LunarPlaceIndexField.type.name: {"type": "keyword"},
        LunarPlaceIndexField.type_code.name: {"type": "keyword"},
        LunarPlaceIndexField.geom_spatial.name: {"type": "geo_shape"},
        LunarPlaceIndexField.geom_str.name: {
            "type": "keyword",
            "doc_values": False,
            "index": False,
        },
        LunarPlaceIndexField.source_type.name: {"type": "keyword"},
        LunarPlaceIndexField.source_path.name: {"type": "keyword"},
        LunarPlaceIndexField.alternate_names.name: {
            "type": "text",
            "fields": {
                LunarPlaceIndexField.alternate_names_keyword.name: {"type": "keyword"},
                LunarPlaceIndexField.alternate_names_lower_keyword.name: {
                    "type": "keyword",
                    "normalizer": "lowercase",
                },
            },
        },
        LunarPlaceIndexField.area_sq_km.name: {"type": "double"},
        LunarPlaceIndexField.diameter_km.name: {"type": "double"},
        LunarPlaceIndexField.center_lat.name: {"type": "double"},
        LunarPlaceIndexField.center_lon.name: {"type": "double"},
        LunarPlaceIndexField.quad_name.name: {"type": "keyword"},
        LunarPlaceIndexField.origin.name: {"type": "text"},
        LunarPlaceIndexField.properties.name: {
            "type": "keyword",
            "doc_values": False,
            "index": False,
        },
    },
}


class LunarPlaceDoc(TypedDict):
    """Represents an indexed lunar geoplace."""

    id: str
    place_name: str
    type: str
    type_code: str
    geom_spatial: dict[str, Any] | None
    geom_str: str
    source_type: str
    source_path: str
    alternate_names: list[str]
    area_sq_km: float | None
    diameter_km: float | None
    center_lat: float | None
    center_lon: float | None
    quad_name: str | None
    origin: str | None
    properties: str


LUNAR_GEOPLACE_INDEX_NAME = "geoplaces-moon"


class LunarPlace(GeoPlace):
    """A lunar feature with its geometry and metadata."""

    type: LunarPlaceType | str
    type_code: str = ""
    source: LunarPlaceSource
    diameter_km: float | None = None
    center_lat: float | None = None
    center_lon: float | None = None
    quad_name: str | None = None
    origin: str | None = None

    @property
    def type_value(self) -> str:
        """Returns the place type as a string."""
        return self.type if isinstance(self.type, str) else self.type.value


class FoundLunarPlace(LunarPlace):
    """A lunar geoplace that has been found in a search query."""

    score: float | None
    sort: list[float] | None

    @staticmethod
    def from_hit(hit: dict[str, Any]) -> "FoundLunarPlace":
        doc: LunarPlaceDoc = hit["_source"]
        place_type: LunarPlaceType | str
        try:
            place_type = LunarPlaceType(doc["type"])
        except ValueError:
            place_type = doc["type"]

        source_type: LunarPlaceSourceType | str
        try:
            source_type = LunarPlaceSourceType(doc["source_type"])
        except ValueError:
            source_type = doc["source_type"]

        return FoundLunarPlace(
            id=doc["id"],
            place_name=doc["place_name"],
            type=place_type,
            type_code=doc.get("type_code", ""),
            geom=geometry_from_geojson(doc["geom_str"]),
            source=LunarPlaceSource(source_type=source_type, source_path=doc["source_path"]),
            alternate_names=doc.get("alternate_names", []),
            area_sq_km=doc.get("area_sq_km"),
            diameter_km=doc.get("diameter_km"),
            center_lat=doc.get("center_lat"),
            center_lon=doc.get("center_lon"),
            quad_name=doc.get("quad_name"),
            origin=doc.get("origin"),
            properties=json.loads(doc.get("properties", "{}")),
            score=hit.get("_score"),
            sort=hit.get("sort"),
        )


def _lunar_geo_place_to_doc(place: LunarPlace) -> LunarPlaceDoc:
    """Converts a LunarPlace model into an OpenSearch document for indexing."""
    return {
        "id": place.id,
        "place_name": place.place_name,
        "type": place.type_value,
        "type_code": place.type_code,
        "geom_str": json.dumps(place.geom.__geo_interface__),
        "geom_spatial": place.geom.__geo_interface__,
        "source_type": place.source.source_type_value,
        "source_path": place.source.source_path,
        "alternate_names": place.alternate_names,
        "area_sq_km": place.area_sq_km,
        "diameter_km": place.diameter_km,
        "center_lat": place.center_lat,
        "center_lon": place.center_lon,
        "quad_name": place.quad_name,
        "origin": place.origin,
        "properties": json.dumps(place.properties),
    }


def _lunar_hit_to_found_geo_place(hit: dict[str, Any]) -> FoundGeoPlace:
    """Converts a raw OpenSearch hit into a generic FoundGeoPlace."""
    doc: LunarPlaceDoc = hit["_source"]
    return FoundGeoPlace(
        id=doc["id"],
        place_name=doc["place_name"],
        type=doc["type"],
        geom=geometry_from_geojson(doc["geom_str"]),
        source=GeoPlaceSource(source_type=doc["source_type"], source_path=doc["source_path"]),
        alternate_names=doc.get("alternate_names", []),
        area_sq_km=doc.get("area_sq_km"),
        properties=json.loads(doc.get("properties", "{}")),
        score=hit.get("_score"),
        sort=hit.get("sort"),
    )


class LunarGeocodeIndex(GeocodeIndexBase[LunarPlace]):
    """OpenSearch index for lunar geospatial places."""

    logger = logging.getLogger(f"{__name__}.LunarGeocodeIndex")
    client: OpenSearch

    def __init__(self, client: OpenSearch | None = None) -> None:
        self.client = client or create_opensearch_client()

    def create_index(self, *, recreate: bool = False) -> None:
        if recreate and self.client.indices.exists(index=LUNAR_GEOPLACE_INDEX_NAME):
            self.logger.warning(
                "Deleting the existing index %s before creating it", LUNAR_GEOPLACE_INDEX_NAME
            )
            self.client.indices.delete(index=LUNAR_GEOPLACE_INDEX_NAME)

        self.logger.info("Creating index %s", LUNAR_GEOPLACE_INDEX_NAME)
        self.client.indices.create(
            index=LUNAR_GEOPLACE_INDEX_NAME,
            body={
                "settings": _LUNAR_INDEX_SETTINGS,
                "mappings": _LUNAR_GEOPLACE_INDEX_MAPPINGS,
            },
        )

    def bulk_index(self, places: list[LunarPlace]) -> None:
        """Index multiple lunar places."""
        bulk_command_lines = [
            json.dumps(bulk_line)
            for place in places
            for bulk_line in [
                {"index": {"_index": LUNAR_GEOPLACE_INDEX_NAME, "_id": place.id}},
                _lunar_geo_place_to_doc(place),
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
            "Searching lunar index with params %s and body %s", params, json.dumps(body)
        )
        start = time()
        os_resp = self.client.search(index=LUNAR_GEOPLACE_INDEX_NAME, params=params, body=body)
        duration = time() - start

        resp_body = os_resp
        hits = resp_body["hits"]["hits"]
        explanations: list[dict[str, Any]] | None = None
        if len(hits) > 0 and "_explanation" in hits[0]:
            explanations = [h["_explanation"] for h in hits]

        places: list[FoundGeoPlace] = [_lunar_hit_to_found_geo_place(hit) for hit in hits]

        self.logger.info(
            "Lunar search took %s seconds with opensearch reporting %s ms",
            duration,
            resp_body["took"],
        )

        return SearchResponse(
            body=resp_body,
            took_ms=resp_body["took"],
            hits=resp_body["hits"]["total"]["value"],
            places=places,
            explanations=explanations,
        )

    @timed_function(logger)
    def get_by_ids(self, ids: Iterable[str]) -> list[LunarPlace]:
        resp = self.client.mget(
            body={"docs": [{"_id": place_id} for place_id in ids]},
            index=LUNAR_GEOPLACE_INDEX_NAME,
        )
        return cast("list[LunarPlace]", [FoundLunarPlace.from_hit(doc) for doc in resp["docs"]])
