from collections.abc import Generator
from typing import Any, TypedDict

import boto3
from e84_geoai_common.util import get_env_var
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection


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


class QueryDSL:
    @staticmethod
    def and_conds(*conds: QueryCondition) -> QueryCondition:
        return {"bool": {"must": conds}}

    @staticmethod
    def or_conds(*conds: QueryCondition) -> QueryCondition:
        return {"bool": {"should": conds}}

    @staticmethod
    def dis_max(*conds: QueryCondition) -> QueryCondition:
        """Combines conjunctions into a dis_max query.

        See https://opensearch.org/docs/latest/query-dsl/compound/disjunction-max/
        """
        return {"dis_max": {"queries": conds}}

    @staticmethod
    def match(
        field: str, text: str, *, fuzzy: bool = False, boost: float | None = None
    ) -> QueryCondition:
        inner_cond: dict[str, str | int | float] = {"query": text}
        if fuzzy:
            inner_cond["fuzziness"] = "AUTO"
        if boost is not None:
            inner_cond["boost"] = boost
        return {"match": {field: inner_cond}}

    @staticmethod
    def term(field: str, value: str, *, boost: float | None = None) -> QueryCondition:
        inner_cond: dict[str, str | float] = {"value": value}
        if boost is not None:
            inner_cond["boost"] = boost

        return {"term": {field: inner_cond}}

    @staticmethod
    def terms(field: str, values: list[str], *, boost: float | None = None) -> QueryCondition:
        if len(values) == 0:
            raise ValueError("Must have one or more values")
        inner_cond: dict[str, float | list[str]] = {field: values}

        if boost is not None:
            inner_cond["boost"] = boost

        return {"terms": inner_cond}


class Hit(TypedDict):
    _id: str
    _source: dict[str, Any]


def scroll_fetch_all(
    client: OpenSearch,
    *,
    index: str,
    query: QueryCondition,
    source_fields: list[str],
) -> Generator[Hit, None, None]:
    """Finds and returns all items using the scroll API."""
    body = {"query": query, "_source": source_fields, "size": 1000}

    # Initialize the scroll
    scroll_resp: dict[str, Any] = client.search(index=index, body=body, params={"scroll": "2m"})

    hits = scroll_resp["hits"]["hits"]
    hits_count = len(hits)
    scroll_id = scroll_resp["_scroll_id"]

    # Continue scrolling until no more hits are returned
    while hits_count > 0:
        scroll_resp = client.scroll(scroll_id=scroll_id, params={"scroll": "2m"})
        hits = scroll_resp["hits"]["hits"]
        hits_count = len(hits)
        scroll_id = scroll_resp["_scroll_id"]
        yield from hits

    # Clear the scroll to free resources
    client.clear_scroll(scroll_id=scroll_id)
