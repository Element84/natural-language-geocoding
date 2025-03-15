import json
from collections.abc import Sequence
from enum import Enum
from textwrap import dedent
from typing import Any

import psycopg2
from e84_geoai_common.geometry import geometry_from_geojson
from pydantic import BaseModel, ConfigDict, Field
from shapely.geometry.base import BaseGeometry


class GeoPlaceType(Enum):
    """The set of different place types that are supported.

    Based on a subset of the Who's On First placetypes.
    """

    continent = "continent"
    ocean = "ocean"
    country = "country"
    empire = "empire"
    locality = "locality"
    dependency = "dependency"
    disputed = "disputed"
    region = "region"
    localadmin = "localadmin"
    borough = "borough"
    county = "county"
    macrocounty = "macrocounty"
    macrohood = "macrohood"
    macroregion = "macroregion"
    marinearea = "marinearea"
    marketarea = "marketarea"
    microhood = "microhood"
    neighbourhood = "neighbourhood"
    postalregion = "postalregion"

    @classmethod
    def to_order_clause(cls) -> str:
        parts = [f"when '{val.value}' then {idx + 1}" for idx, val in enumerate(cls)]
        parts_joined = "\n                ".join(parts)
        return f"case type\n                {parts_joined}\n            end"


class GeoPlaceSource(Enum):
    wof = "wof"


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
    source: GeoPlaceSource
    source_path: str
    source_id: int
    alternate_names: list[str] = Field(default_factory=list)
    properties: dict[str, Any]

    # TODO temporarily added for testing
    similarity: float | None = None


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

    def delete_by_source_path(self, source: GeoPlaceSource, source_path: str) -> None:
        with self._conn.cursor() as cur:
            sql = "DELETE FROM geo_places WHERE source = %s and source_path = %s"
            cur.execute(sql, [source.value, source_path])

        self._conn.commit()

    def insert_geoplaces(self, places: Sequence[GeoPlace]) -> None:
        """Insert multiple GeoPlace objects into the database."""
        with self._conn.cursor() as cur:
            for place in places:
                cur.execute(
                    """
                    INSERT INTO geo_places (
                        name,
                        type,
                        geom,
                        alternative_names,
                        properties,
                        source,
                        source_path,
                        source_id
                    )
                    VALUES (
                        %s,
                        %s,
                        ST_GeomFromGeoJSON(%s),
                        %s,
                        %s::jsonb,
                        %s,
                        %s,
                        %s
                    )
                    """,
                    (
                        place.name,
                        place.type.value,
                        json.dumps(place.geom.__geo_interface__),
                        place.alternate_names,
                        json.dumps(place.properties),
                        place.source.value,
                        place.source_path,
                        place.source_id,
                    ),
                )
        self._conn.commit()


##########################################################
# Code for testing

import os

from e84_geoai_common.debugging import display_geometry

conn_str = os.getenv("GEOCODE_DB_CONN_STR")
if conn_str is None:
    raise Exception("GEOCODE_DB_CONN_STR must be set")
db = GeocodeDB(conn_str)


def search_by_source_id(source_id: int) -> GeoPlace | None:
    sql = dedent("""
        select
            id,
            name,
            type,
            ST_AsGeoJSON(geom),
            alternative_names,
            properties,
            source,
            source_path,
            source_id
        from geo_places
        where source_id = %s
    """).strip()
    with db._conn.cursor() as cur:
        cur.execute(sql, [source_id])
        row: tuple[int, str, str, str, list[str], dict[str, Any], str, str, int] | None = (
            cur.fetchone()
        )
    if row:
        return GeoPlace(
            id=row[0],
            name=row[1],
            type=GeoPlaceType(row[2]),
            geom=geometry_from_geojson(row[3]),
            alternate_names=row[4],
            properties=row[5],
            source=GeoPlaceSource(row[6]),
            source_path=row[7],
            source_id=row[8],
        )
    return None


def search_by_name(place_type: GeoPlaceType, place_name: str, *, limit: int = 10) -> list[GeoPlace]:
    sql = dedent(f"""
        select * from (
            select
                id,
                name,
                type,
                ST_AsGeoJSON(geom),
                alternative_names,
                properties,
                source,
                source_path,
                source_id,
                similarity(name, %s) similarity
            from geo_places
            where type = %s and name %% %s
        ) sub
        where similarity > 0.3
        order by
            similarity desc,
            {GeoPlaceType.to_order_clause()}
        limit %s;
    """).strip()  # noqa: S608
    with db._conn.cursor() as cur:
        cur.execute(sql, [place_name, place_type.value, place_name, limit])
        rows: list[tuple[int, str, str, str, list[str], dict[str, Any], str, str, int, float]] = (
            cur.fetchall()
        )
    return [
        GeoPlace(
            id=row[0],
            name=row[1],
            type=GeoPlaceType(row[2]),
            geom=geometry_from_geojson(row[3]),
            alternate_names=row[4],
            properties=row[5],
            source=GeoPlaceSource(row[6]),
            source_path=row[7],
            source_id=row[8],
            similarity=row[9],
        )
        for row in rows
    ]


# TODO things that need to be done
# - Search by a certain type
# - Search where the place is inside of another area
# - Rivers are missing
# - Searching by "Atlantic" finds the ocean as two separate entries

# Things that need to change
# - Find rivers to ingest
# - the llm should indicate a type like ocean, etc
# - The llm should also be able to provide an identifier for what it's within
#   - We need to pull out what it's within into a separate queryable list

db.reconnect()

results = search_by_name(GeoPlaceType.country, "United States")


display_geometry([results[0].geom])
display_geometry([results[1].geom])
display_geometry([results[2].geom])
display_geometry([results[3].geom])

print(json.dumps(results[0].properties, indent=2))

display_geometry([search_by_source_id(102191575).geom])
