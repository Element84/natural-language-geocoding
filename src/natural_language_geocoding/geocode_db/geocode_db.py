import json
from collections.abc import Sequence

import psycopg2
from pydantic import BaseModel, ConfigDict, Field
from shapely.geometry.base import BaseGeometry


class GeoPlace(BaseModel):
    model_config = ConfigDict(
        strict=True, extra="forbid", frozen=True, arbitrary_types_allowed=True
    )

    id: int | None = None
    name: str
    type: str
    geom: BaseGeometry
    alternate_names: list[str] = Field(default_factory=list)


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
                    INSERT INTO geo_places (name, type, geom, alternative_names)
                    VALUES (
                        %s,
                        %s,
                        ST_GeomFromGeoJSON(%s),
                        %s
                    )
                    """,
                    (
                        place.name,
                        place.type,
                        json.dumps(place.geom.__geo_interface__),
                        place.alternate_names,
                    ),
                )
        self._conn.commit()
