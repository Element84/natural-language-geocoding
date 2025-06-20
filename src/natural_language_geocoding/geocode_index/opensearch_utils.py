"""Contains utility functions for working with opensearch."""

import os
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
    host = get_env_var("GEOCODE_INDEX_HOST")
    port = int(get_env_var("GEOCODE_INDEX_PORT", "443"))
    region = get_env_var("GEOCODE_INDEX_REGION")
    username = os.getenv("GEOCODE_INDEX_USERNAME")
    password = os.getenv("GEOCODE_INDEX_PASSWORD")

    is_localhost = host in {"localhost", "host.docker.internal"}

    if username and password:
        auth = (username, password)
    elif not is_localhost:
        credentials = boto3.Session().get_credentials()
        auth = AWSV4SignerAuth(credentials, region, "es")
    else:
        auth = None

    return OpenSearch(
        hosts=[{"host": host, "port": port}],
        use_ssl=True,
        verify_certs=not is_localhost,
        connection_class=RequestsHttpConnection,
        pool_maxsize=20,
        http_auth=auth,
    )


QueryCondition = dict[str, Any]


class IndexField(Enum):
    """Represents a single indexed field in Opensearch."""

    # The parent field in opensearch if this is for a nested document.
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
        """Returns the path to the node."""
        if self.parent:
            return f"{self.parent}.{self._name}"
        return self._name

    @property
    def is_nested(self) -> bool:
        """Returns true if this is for a nested document."""
        return self.parent is not None


class QueryDSL:
    """Utility functions for creating OpenSearch query objects for complex search operations.

    This class provides static methods that help build structured query conditions
    for OpenSearch, allowing for readable and maintainable query composition.
    """

    @staticmethod
    def bool_cond(
        *,
        must_conds: Sequence[QueryCondition] | None = None,
        must_not_conds: Sequence[QueryCondition] | None = None,
        should_conds: Sequence[QueryCondition] | None = None,
        filter_cond: QueryCondition | None = None,
    ) -> QueryCondition:
        """Creates a boolean query condition that combines multiple clauses with boolean logic.

        Args:
            must_conds: Sequence of query conditions that must match (AND logic)
            must_not_conds: Sequence of query conditions that must not match (NOT logic)
            should_conds: Sequence of query conditions where at least one should match (OR logic)
            filter_cond: Query condition that must match, but doesn't contribute to the score

        Returns:
            A properly formatted boolean query condition for OpenSearch

        See https://opensearch.org/docs/latest/query-dsl/compound/bool/
        """
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
        """Combines multiple query conditions with logical AND.

        Args:
            *conds: One or more query conditions to be combined with AND logic

        Returns:
            A boolean query where all conditions must match
        """
        return QueryDSL.bool_cond(must_conds=conds)

    @staticmethod
    def or_conds(*conds: QueryCondition) -> QueryCondition:
        """Combines multiple query conditions with logical OR.

        Args:
            *conds: One or more query conditions where at least one should match

        Returns:
            A boolean query where at least one condition should match
        """
        return QueryDSL.bool_cond(should_conds=conds)

    @staticmethod
    def dis_max(*conds: QueryCondition) -> QueryCondition:
        """Combines conjunctions into a dis_max query.

        The dis_max query generates the union of documents produced by its subqueries,
        and scores each document with the maximum score for that document across all subqueries.

        Args:
            *conds: The query conditions to combine

        Returns:
            A dis_max query condition

        See https://opensearch.org/docs/latest/query-dsl/compound/disjunction-max/
        """
        return {"dis_max": {"queries": conds}}

    @staticmethod
    def match(
        field: IndexField, text: str, *, fuzzy: bool = False, boost: float | None = None
    ) -> QueryCondition:
        """Creates a match query condition for text searches.

        Args:
            field: The index field to search in
            text: The text to search for
            fuzzy: Whether to enable fuzzy matching (tolerates typos)
            boost: Optional relevance boost factor for this condition

        Returns:
            A match query condition for the specified field and text
        """
        inner_cond: dict[str, str | int | float] = {"query": text}
        if fuzzy:
            inner_cond["fuzziness"] = "AUTO"
        if boost is not None:
            inner_cond["boost"] = boost
        return {"match": {field.path: inner_cond}}

    @staticmethod
    def term(field: IndexField, value: str, *, boost: float | None = None) -> QueryCondition:
        """Creates a term query condition for exact match searches.

        Term queries are not analyzed and will only match if the field contains exactly
        the specified value.

        Args:
            field: The index field to search in
            value: The exact value to match
            boost: Optional relevance boost factor for this condition

        Returns:
            A term query condition for the specified field and value
        """
        inner_cond: dict[str, str | float] = {"value": value}
        if boost is not None:
            inner_cond["boost"] = boost

        return {"term": {field.path: inner_cond}}

    @staticmethod
    def terms(
        field: IndexField, values: list[str], *, boost: float | None = None
    ) -> QueryCondition:
        """Creates a terms query condition for matching multiple possible values.

        This is equivalent to a series of term queries combined with OR logic.

        Args:
            field: The index field to search in
            values: List of values where any can match
            boost: Optional relevance boost factor for this condition

        Returns:
            A terms query condition for the specified field and values

        Raises:
            ValueError: If the values list is empty
        """
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
        """Creates a geo_shape query condition for spatial queries.

        Args:
            field: The index field containing geo shapes to query against
            geom: The geometry to use for the spatial query
            relation: The spatial relationship to test (INTERSECTS, CONTAINS, WITHIN, or DISJOINT)

        Returns:
            A geo_shape query condition for the specified field and geometry
        """
        return {"geo_shape": {field.path: {"shape": geom.__geo_interface__, "relation": relation}}}


class Hit(TypedDict):
    """Represents the results of a single match of an opesearch query."""

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


def ordered_values_to_sort_cond(field: IndexField, values: Sequence[str | Enum]) -> dict[str, Any]:
    """Generates a sort condition for a field based on a predefined order of known values.

    Note that sorting this way can be slow and it's better to index a new field with the integer
    values instead and sort by that. It does require reindexing when changing sort order though.
    """
    value_strs = [(v if isinstance(v, str) else v.value) for v in values]

    order_values = [f"    '{value}': {index}" for index, value in enumerate(value_strs)]
    order_values_str = "\n,".join(order_values)

    sort_cond_script = dedent(
        f"""
            def fieldOrder = [
                {order_values_str}
            ];
            if (fieldOrder.containsKey(doc['{field.value}'].value)) {{
                return fieldOrder[doc['{field.value}'].value];
            }} else {{
                return 999;
            }}
        """.strip()
    )

    return {
        "_script": {
            "type": "number",
            "script": {
                "source": sort_cond_script,
                "lang": "painless",
            },
            "order": "asc",
        }
    }
