import json
from typing import Any

import boto3
from e84_geoai_common.geometry import geometry_from_geojson, geometry_to_geojson
from e84_geoai_common.util import get_env_var
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection
from pydantic import BaseModel, ConfigDict

from natural_language_geocoding.geocode_index.geoplace import (
    GeoPlace,
    GeoPlaceSource,
    GeoPlaceSourceType,
    GeoPlaceType,
    Hierarchy,
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
        "name": {"type": "text"},
        "type": {"type": "keyword"},
        # We may not need to search it as a geometry
        # "geom": {"type": "geo_shape"},
        "geom": {"type": "keyword", "doc_values": False, "index": False},
        "source_id": {"type": "long"},
        "source_type": {"type": "keyword"},
        "source_path": {"type": "keyword"},
        "alternate_names": {"type": "text"},
        "continent_id": {"type": "long"},
        "country_id": {"type": "long"},
        "empire_id": {"type": "long"},
        "locality_id": {"type": "long"},
        "macroregion_id": {"type": "long"},
        "region_id": {"type": "long"},
        "properties": {"type": "keyword", "doc_values": False, "index": False},
    },
}

_GEOPLACE_INDEX_NAME = "geoplaces"


def _geo_place_to_doc(geoplace: GeoPlace) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "id": geoplace.id,
        "name": geoplace.name,
        "type": geoplace.type.value,
        "geom": geometry_to_geojson(geoplace.geom),
        "source_id": geoplace.source_id,
        "source_type": geoplace.source.source_type.value,
        "source_path": geoplace.source.source_path,
        "alternate_names": geoplace.alternate_names,
        "properties": json.dumps(geoplace.properties),
    }
    if hierarchy := geoplace.hierarchy:
        doc["continent_id"] = hierarchy.continent_id
        doc["country_id"] = hierarchy.country_id
        doc["locality_id"] = hierarchy.locality_id
        doc["macroregion_id"] = hierarchy.macroregion_id
        doc["region_id"] = hierarchy.region_id
        doc["empire_id"] = hierarchy.empire_id
    return doc


def _doc_to_geo_place(doc: dict[str, Any]) -> GeoPlace:
    continent_id = doc.get("continent_id")
    country_id = doc.get("country_id")
    locality_id = doc.get("locality_id")
    macroregion_id = doc.get("macroregion_id")
    region_id = doc.get("region_id")
    empire_id = doc.get("empire_id")

    if continent_id or country_id or locality_id or macroregion_id or region_id:
        hierarchy = Hierarchy(
            continent_id=continent_id,
            country_id=country_id,
            locality_id=locality_id,
            macroregion_id=macroregion_id,
            region_id=region_id,
            empire_id=empire_id,
        )
    else:
        hierarchy = None

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
        hierarchy=hierarchy,
    )


class SearchResponse(BaseModel):
    model_config = ConfigDict(
        strict=True,
        extra="forbid",
        frozen=True,
    )
    took_ms: int
    hits: int
    places: list[GeoPlace]

    @staticmethod
    def from_search_resp(body: dict[str, Any]) -> "SearchResponse":
        return SearchResponse(
            took_ms=body["took"],
            hits=body["hits"]["total"]["value"],
            places=[_doc_to_geo_place(hit["_source"]) for hit in body["hits"]["hits"]],
        )


class GeocodeIndex:
    def __init__(self) -> None:
        host = get_env_var("GEOCODE_INDEX_HOST")
        region = get_env_var("GEOCODE_INDEX_REGION")
        credentials = boto3.Session().get_credentials()
        auth = AWSV4SignerAuth(credentials, region, "es")

        self.client = OpenSearch(
            hosts=[{"host": host, "port": 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20,
        )

    def create_index(self, *, recreate: bool = False) -> None:
        if recreate and self.client.indices.exists(index=_GEOPLACE_INDEX_NAME):
            self.client.indices.delete(index=_GEOPLACE_INDEX_NAME)

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
            print(json.dumps(resp))
            raise Exception("There were errors in the bulk index")
