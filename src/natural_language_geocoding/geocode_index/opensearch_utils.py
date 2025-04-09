from collections.abc import Generator, Sequence
from enum import Enum
from textwrap import dedent
from typing import Any, Literal, TypedDict

import boto3
from e84_geoai_common.util import get_env_var
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection
from shapely.geometry.base import BaseGeometry


def create_opensearch_client() -> OpenSearch:
    """Creates an opensearch client object."""
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


QueryCondition = dict[str, Any]


class IndexField(Enum):
    """TODO docs."""

    parent: str | None
    _name: str

    def __init__(self, parent_or_name: str, subname: str | None = None) -> None:
        if subname:
            self.parent = parent_or_name
            self._name = subname
        else:
            self._name = parent_or_name
            self.parent = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def path(self) -> str:
        """TODO docs."""
        if self.parent:
            return f"{self.parent}.{self._name}"
        return self._name

    @property
    def is_nested(self) -> bool:
        """TODO docs."""
        return self.parent is not None


class QueryDSL:
    """TODO docs."""

    @staticmethod
    def bool_cond(
        *,
        must_conds: Sequence[QueryCondition] | None = None,
        must_not_conds: Sequence[QueryCondition] | None = None,
        should_conds: Sequence[QueryCondition] | None = None,
        filter_cond: QueryCondition | None = None,
    ) -> QueryCondition:
        """See https://opensearch.org/docs/latest/query-dsl/compound/bool/."""
        bool_dict: dict[str, Any] = {}

        if should_conds:
            bool_dict["should"] = should_conds
        if must_conds:
            bool_dict["must"] = must_conds
        if must_not_conds:
            bool_dict["must_not"] = must_not_conds
        if filter_cond:
            bool_dict["filter"] = filter_cond

        return {"bool": bool_dict}

    @staticmethod
    def and_conds(*conds: QueryCondition) -> QueryCondition:
        """TODO docs."""
        return QueryDSL.bool_cond(must_conds=conds)

    @staticmethod
    def or_conds(*conds: QueryCondition) -> QueryCondition:
        """TODO docs."""
        return QueryDSL.bool_cond(should_conds=conds)

    @staticmethod
    def dis_max(*conds: QueryCondition) -> QueryCondition:
        """Combines conjunctions into a dis_max query.

        See https://opensearch.org/docs/latest/query-dsl/compound/disjunction-max/
        """
        return {"dis_max": {"queries": conds}}

    @staticmethod
    def match(
        field: IndexField, text: str, *, fuzzy: bool = False, boost: float | None = None
    ) -> QueryCondition:
        """TODO docs."""
        inner_cond: dict[str, str | int | float] = {"query": text}
        if fuzzy:
            inner_cond["fuzziness"] = "AUTO"
        if boost is not None:
            inner_cond["boost"] = boost
        return {"match": {field.path: inner_cond}}

    @staticmethod
    def term(field: IndexField, value: str, *, boost: float | None = None) -> QueryCondition:
        """TODO docs."""
        inner_cond: dict[str, str | float] = {"value": value}
        if boost is not None:
            inner_cond["boost"] = boost

        return {"term": {field.path: inner_cond}}

    @staticmethod
    def terms(
        field: IndexField, values: list[str], *, boost: float | None = None
    ) -> QueryCondition:
        """TODO docs."""
        if len(values) == 0:
            raise ValueError("Must have one or more values")
        inner_cond: dict[str, float | list[str]] = {field.path: values}

        if boost is not None:
            inner_cond["boost"] = boost

        return {"terms": inner_cond}

    @staticmethod
    def geo_shape(
        field: IndexField,
        geom: BaseGeometry,
        *,
        relation: Literal["CONTAINS", "WITHIN", "DISJOINT", "INTERSECTS"] = "INTERSECTS",
    ) -> QueryCondition:
        """TODO docs."""
        return {"geo_shape": {field.path: {"shape": geom.__geo_interface__, "relation": relation}}}


class Hit(TypedDict):
    """TODO docs."""

    _id: str
    _source: dict[str, Any]


def scroll_fetch_all(
    client: OpenSearch,
    *,
    index: str,
    query: QueryCondition,
    source_fields: list[IndexField],
) -> Generator[Hit, None, None]:
    """Finds and returns all items using the scroll API."""
    body = {"query": query, "_source": [f.value for f in source_fields], "size": 1000}

    # Initialize the scroll
    scroll_resp: dict[str, Any] = client.search(index=index, body=body, params={"scroll": "2m"})

    hits = scroll_resp["hits"]["hits"]
    hits_count = len(hits)
    scroll_id = scroll_resp["_scroll_id"]
    yield from hits

    # Continue scrolling until no more hits are returned
    while hits_count > 0:
        scroll_resp = client.scroll(scroll_id=scroll_id, params={"scroll": "2m"})
        hits = scroll_resp["hits"]["hits"]
        hits_count = len(hits)
        scroll_id = scroll_resp["_scroll_id"]
        yield from hits

    # Clear the scroll to free resources
    client.clear_scroll(scroll_id=scroll_id)


def ordered_values_to_sort_cond(field: IndexField, values: Sequence[str]) -> dict[str, Any]:
    """Generates a sort condition for a field based on a predefined order of known values.

    Note that sorting this way can be slow and it's better to index a new field with the integer
    values instead and sort by that. It does require reindexing when changing sort order though.
    """
    order_values = [f"    '{value}': {index}" for index, value in enumerate(values)]
    order_values_str = "\n,".join(order_values)

    sort_cond_script = dedent(
        f"""
            def typeOrder = [
                {order_values_str}
            ];
            return typeOrder.containsKey(doc['type'].value) ? typeOrder[doc['type'].value] : 999;
        """.strip()
    )

    return {
        "_script": {
            field.value: "number",
            "script": {
                "source": sort_cond_script,
                "lang": "painless",
            },
            "order": "asc",
        }
    }
