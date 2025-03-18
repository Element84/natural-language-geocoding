import json
from typing import Any, Literal

import boto3
from e84_geoai_common.geometry import geometry_from_geojson, geometry_to_geojson
from e84_geoai_common.util import get_env_var, timed_function
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection
from pydantic import BaseModel, ConfigDict
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.geocode_index.geoplace import (
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
    if hierarchies := geoplace.hierarchies:
        doc["hierarchies"] = [h.model_dump() for h in hierarchies]
    return doc


def _doc_to_geo_place(doc: dict[str, Any]) -> GeoPlace:
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


class SearchRequest(BaseModel):
    model_config = ConfigDict(
        strict=True,
        extra="forbid",
        frozen=True,
    )
    size: int = 10
    start_index: int = 0
    sort: list[tuple[str, Literal["asc", "desc"]]] | None = None
    query: dict[str, Any]


class GeocodeIndex:
    def __init__(self) -> None:
        # TODO include these env vars as part of the documentation
        host = get_env_var("GEOCODE_INDEX_HOST")
        port = int(get_env_var("GEOCODE_INDEX_PORT", "443"))
        region = get_env_var("GEOCODE_INDEX_REGION")
        credentials = boto3.Session().get_credentials()
        auth = AWSV4SignerAuth(credentials, region, "es")

        if host == "localhost":
            # Allow tunneling for easy local testing.
            self.client = OpenSearch(
                hosts=[{"host": host, "port": port}],
                use_ssl=True,
                verify_certs=False,
                connection_class=RequestsHttpConnection,
                pool_maxsize=20,
            )
        else:
            self.client = OpenSearch(
                hosts=[{"host": host, "port": port}],
                http_auth=auth,
                use_ssl=True,
                verify_certs=host != "localhost",
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
            print(json.dumps(resp))  # noqa: T201
            raise Exception("There were errors in the bulk index")

    # TODO Update the timed function to take a logger
    @timed_function
    def search(self, request: SearchRequest) -> SearchResponse:
        body: dict[str, Any] = {
            "query": request.query,
            "size": request.size,
            "from": request.start_index,
        }
        if request.sort:
            body["sort"] = [f"{field}:{direction}" for field, direction in request.sort]

        resp = self.client.search(index=_GEOPLACE_INDEX_NAME, body=body)
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
    def match(field: str, text: str, *, fuzzy: bool = False) -> QueryCondition:
        inner_cond = {"query": text}
        if fuzzy:
            inner_cond["fuzziness"] = "AUTO"
        return {"match": {field: inner_cond}}

    @staticmethod
    def term(field: str, value: str) -> QueryCondition:
        return {"term": {field: {"value": value}}}


class GeocodeIndexPlaceLookup(PlaceLookup):
    _index: GeocodeIndex

    def __init__(self) -> None:
        self._index = GeocodeIndex()

    def _find_by_name_and_type(
        self, name: str, place_type: GeoPlaceType, other_conditions: list[QueryCondition]
    ) -> str:
        request = SearchRequest(
            size=5,
            query=QueryDSL.and_conds(
                QueryDSL.match("name", name),
                QueryDSL.term("type", place_type.value),
                *other_conditions,
            ),
        )
        resp = self._index.search(request)
        if len(resp.places) > 0:
            return resp.places[0].id
        raise Exception(
            f"Unable find place with name [{name}] type [{place_type}] and other conditions "
            f"[{json.dumps(other_conditions)}]"
        )

    @timed_function
    def search_for_places(  # noqa: PLR0913
        self,
        *,
        name: str,
        place_type: GeoPlaceType | None = None,
        in_continent: str | None = None,
        in_country: str | None = None,
        in_region: str | None = None,
        limit: int = 5,
    ) -> list[GeoPlace]:
        conditions: list[QueryCondition] = [
            QueryDSL.or_conds(
                QueryDSL.match("name", name),
                QueryDSL.match("alternate_names", name),
            )
        ]
        if place_type:
            conditions.append(QueryDSL.term("type", place_type.value))

        within_conds: list[QueryCondition] = []

        if in_continent:
            continent_id = self._find_by_name_and_type(in_continent, GeoPlaceType.continent, [])
            within_conds.append(QueryDSL.term("hierarchies.continent_id", continent_id))
        if in_country:
            country_id = self._find_by_name_and_type(in_country, GeoPlaceType.country, within_conds)
            within_conds.append(QueryDSL.term("hierarchies.country_id", country_id))
        if in_region:
            region_id = self._find_by_name_and_type(in_region, GeoPlaceType.region, within_conds)
            within_conds.append(QueryDSL.term("hierarchies.region_id", region_id))

        request = SearchRequest(
            size=limit,
            query=QueryDSL.and_conds(*conditions, *within_conds),
        )
        resp = self._index.search(request)
        return resp.places

    @timed_function
    def search(
        self,
        *,
        name: str,
        place_type: GeoPlaceType | None = None,
        in_continent: str | None = None,
        in_country: str | None = None,
        in_region: str | None = None,
    ) -> BaseGeometry:
        places = self.search_for_places(
            name=name,
            place_type=place_type,
            in_continent=in_continent,
            in_country=in_country,
            in_region=in_region,
        )
        if len(places) > 0:
            return places[0].geom
        raise Exception(
            f"Unable find place with name [{name}] "
            f"type [{place_type}] "
            f"in_continent [{in_continent}] "
            f"in_country [{in_country}] "
            f"in_region [{in_region}] "
        )


## Code for testing
# ruff: noqa: ERA001
# lookup = GeocodeIndexPlaceLookup()

# places = lookup.search_for_places(name="Florida", place_type=GeoPlaceType.region, limit=20)

# print_places_as_table(places)


# display_geometry([places[0].geom])
# display_geometry([places[1].geom])
# display_geometry([places[2].geom])
# display_geometry([places[3].geom])
# display_geometry([places[4].geom])
