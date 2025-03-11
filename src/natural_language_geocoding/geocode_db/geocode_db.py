import json
from collections.abc import Sequence
from enum import Enum
from typing import Any

import psycopg2
from pydantic import BaseModel, ConfigDict, Field
from shapely.geometry.base import BaseGeometry


class GeoPlaceType(Enum):
    """The set of different place types that are supported.

    Based on a subset of the Who's On First placetypes.
    """

    borough = "borough"
    continent = "continent"
    country = "country"
    county = "county"
    dependency = "dependency"
    disputed = "disputed"
    empire = "empire"
    localadmin = "localadmin"
    locality = "locality"
    macrocounty = "macrocounty"
    macrohood = "macrohood"
    macroregion = "macroregion"
    marinearea = "marinearea"
    marketarea = "marketarea"
    microhood = "microhood"
    neighbourhood = "neighbourhood"
    ocean = "ocean"
    postalregion = "postalregion"
    region = "region"


class GeoPlace(BaseModel):
    model_config = ConfigDict(
        strict=True,
        extra="forbid",
        frozen=True,
        arbitrary_types_allowed=True,
        json_encoders={GeoPlaceType: lambda x: x.value},
    )

    id: int | None = None
    name: str
    type: GeoPlaceType
    geom: BaseGeometry
    alternate_names: list[str] = Field(default_factory=list)
    properties: dict[str, Any]


class GeocodeDB:
    _conn_string: str
    _conn: psycopg2.extensions.connection

    def __init__(self, connection_string: str) -> None:
        self._conn_string = connection_string
        self._conn = psycopg2.connect(self._conn_string)

    def reconnect(self) -> None:
        self._conn.close()
        self._conn = psycopg2.connect(self._conn_string)

    def _test_connection(self, *, rethrow: bool = False) -> bool:
        try:
            with self._conn.cursor() as cursor:
                cursor.execute("select 1")
        except psycopg2.DatabaseError:
            if rethrow:
                raise
            return False
        else:
            return True

    def _ensure_connected(self) -> None:
        if self._conn.closed == 1:
            self.reconnect()
        if not self._test_connection():
            self.reconnect()
            self._test_connection(rethrow=True)

    def insert_geoplaces(self, places: Sequence[GeoPlace]) -> None:
        """Insert multiple GeoPlace objects into the database."""
        with self._conn.cursor() as cur:
            for place in places:
                cur.execute(
                    """
                    INSERT INTO geo_places (name, type, geom, alternative_names, properties)
                    VALUES (
                        %s,
                        %s,
                        ST_GeomFromGeoJSON(%s),
                        %s,
                        %s::jsonb
                    )
                    """,
                    (
                        place.name,
                        place.type.value,
                        json.dumps(place.geom.__geo_interface__),
                        place.alternate_names,
                        json.dumps(place.properties),
                    ),
                )
        self._conn.commit()


#     def search_by_name(
#         self, name: str
#     ) -> list[GeoPlace]:
#         # Select fields
#         select = "select id, uri, ST_AsGeoJSON(polygon) from queryable_earth_chips"
#         sql_args: list[str | int] = []

#         # Where clause
#         if geometry is not None:
#             where = "where ST_Intersects(polygon, ST_GeomFromGeoJSON(%s))"
#             sql_args.append(geometry_to_geojson(geometry))
#         else:
#             where = ""

#         # Order by embedding match
#         order_by = "ORDER BY embedding <-> %s"
#         embedding_floats: list[float] = embeddings.tolist()[0]
#         embedding_float_str = "[" + ",".join([str(e) for e in embedding_floats]) + "]"
#         sql_args.append(embedding_float_str)

#         # Limit to number of matches requested
#         limit = "limit %s"
#         sql_args.append(k)

#         sql = " ".join([select, where, order_by, limit])

#         self._ensure_connected()
#         with self._conn.cursor() as cursor:
#             cursor.execute(sql, sql_args)
#             rows: list[tuple[int, str, str]] = cursor.fetchall()

#         return [
#             ChipFeature.create(id=id, uri=uri, polygon_json=polygon_json)
#             for id, uri, polygon_json in rows
#         ]


# class GeocodeDbPlaceLookup(PlaceLookup):

#     db: GeocodeDB

#     def search(self, name: str) -> BaseGeometry:
