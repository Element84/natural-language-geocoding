"""Provides a cache of place name data for fast lookups of hierarchical relationships."""

import json
from collections.abc import Collection
from pathlib import Path

from e84_geoai_common.util import timed_function

from natural_language_geocoding.geocode_index.geoplace import GeoPlaceType, Hierarchy
from natural_language_geocoding.geocode_index.index import GEOPLACE_INDEX_NAME, GeoPlaceIndexField
from natural_language_geocoding.geocode_index.opensearch_utils import (
    QueryDSL,
    create_opensearch_client,
    scroll_fetch_all,
)


def _append_to_dict[K](d: dict[K, set[str]], key: K, item: str) -> None:
    if key not in d:
        d[key] = set()
    d[key].add(item)


class HierchicalPlaceCache:
    """A in memory cache for looking up geographic places by name, type, and hierarchy."""

    # The source of truth for which ids are in the cache. Will contain every id of every place
    # mapped to the hierarchies of that place
    _id_to_name_place_hierarchies: dict[str, tuple[str, GeoPlaceType, list[Hierarchy]]]

    # Fast reverse lookups of name and place type with other parent ids.
    _name_place_to_ids: dict[tuple[str, GeoPlaceType], set[str]]
    _name_place_continent_to_ids: dict[tuple[str, GeoPlaceType, str], set[str]]
    _name_place_country_to_ids: dict[tuple[str, GeoPlaceType, str], set[str]]
    _name_place_continent_country_to_ids: dict[tuple[str, GeoPlaceType, str, str], set[str]]

    def __init__(self) -> None:
        self._id_to_name_place_hierarchies = {}
        self._name_place_to_ids = {}
        self._name_place_continent_to_ids = {}
        self._name_place_country_to_ids = {}
        self._name_place_continent_country_to_ids = {}

    def add(
        self,
        feature_id: str,
        name: str,
        place_type: GeoPlaceType,
        hierarchies: list[Hierarchy],
    ) -> None:
        """Add a geographic place to the cache.

        Args:
            feature_id: Unique identifier for the place
            name: The place name
            place_type: The type of place (country, region, etc.)
            hierarchies: List of hierarchical relationships this place belongs to

        Raises:
            Exception: If the feature_id already exists in the cache
        """
        # A sanity check
        if feature_id in self._id_to_name_place_hierarchies:
            raise Exception(
                f"Unexpected duplicate feature id {feature_id} for {name} {place_type.value}"
            )
        self._id_to_name_place_hierarchies[feature_id] = (name, place_type, hierarchies)
        _append_to_dict(self._name_place_to_ids, (name, place_type), feature_id)

        for hierarchy in hierarchies:
            continent_id = hierarchy.continent_id
            country_id = hierarchy.country_id

            if continent_id:
                _append_to_dict(
                    self._name_place_continent_to_ids,
                    (name, place_type, continent_id),
                    feature_id,
                )
            if country_id:
                _append_to_dict(
                    self._name_place_country_to_ids,
                    (name, place_type, country_id),
                    feature_id,
                )
            if continent_id and country_id:
                _append_to_dict(
                    self._name_place_continent_country_to_ids,
                    (name, place_type, continent_id, country_id),
                    feature_id,
                )

    def find_ids(
        self,
        *,
        name: str,
        place_type: GeoPlaceType,
        continent_ids: Collection[str] | None = None,
        country_ids: Collection[str] | None = None,
    ) -> set[str]:
        """Find feature IDs matching the given name, place type, and parents.

        Args:
            name: Place name to search for
            place_type: Type of place to search for
            continent_ids: Optional filter to limit results to specific continents
            country_ids: Optional filter to limit results to specific countries

        Returns:
            A set of feature IDs matching the search criteria

        Raises:
            ValueError: If empty collections are provided for continent_ids or country_ids
        """
        # Validate args
        if continent_ids is not None and len(continent_ids) == 0:
            raise ValueError("if continent_ids are provided at least one must be specified")
        if country_ids is not None and len(country_ids) == 0:
            raise ValueError("if country_ids are provided at least one must be specified")
        if continent_ids:
            if country_ids:
                matches = {
                    fid
                    for continent_id in continent_ids
                    for country_id in country_ids
                    for fid in self._name_place_continent_country_to_ids.get(
                        (name, place_type, continent_id, country_id), set()
                    )
                }
            else:
                matches = {
                    fid
                    for continent_id in continent_ids
                    for fid in self._name_place_continent_to_ids.get(
                        (name, place_type, continent_id), set()
                    )
                }
        elif country_ids:
            matches = {
                fid
                for country_id in country_ids
                for fid in self._name_place_country_to_ids.get(
                    (name, place_type, country_id), set()
                )
            }
        else:
            matches = self._name_place_to_ids.get((name, place_type), set())
        return matches

    def to_json(self, *, indent: int | str | None = None) -> str:
        """Serialize the cache to a JSON string.

        Args:
            indent: Optional indentation to use for formatting the JSON

        Returns:
            JSON string representation of the cache data
        """
        rows = [
            (
                feature_id,
                name,
                place_type.value,
                [h.model_dump(exclude_none=True) for h in hierarchies],
            )
            for feature_id, (
                name,
                place_type,
                hierarchies,
            ) in self._id_to_name_place_hierarchies.items()
        ]
        return json.dumps(rows, indent=indent)

    @staticmethod
    def from_json(json_str: str) -> "HierchicalPlaceCache":
        """Create a cache instance from a JSON string.

        Args:
            json_str: JSON serialized cache data

        Returns:
            A new HierchicalPlaceCache populated with the deserialized data
        """
        dicts = HierchicalPlaceCache()
        rows = json.loads(json_str)
        for feature_id, name, place_type_str, hierarchies_data in rows:
            place_type = GeoPlaceType(place_type_str)
            hierarchies = [Hierarchy.model_validate(h) for h in hierarchies_data]
            dicts.add(feature_id, name, place_type, hierarchies)
        return dicts


@timed_function
def _populate() -> HierchicalPlaceCache:
    """Populate a new cache by querying the geoplace index in OpenSearch.

    Returns:
        A populated HierchicalPlaceCache with continents, countries, and regions
    """
    # Otherwise, populate from OpenSearch
    client = create_opensearch_client()
    cache = HierchicalPlaceCache()

    for hit in scroll_fetch_all(
        client,
        index=GEOPLACE_INDEX_NAME,
        query=QueryDSL.terms(
            GeoPlaceIndexField.type,
            [
                GeoPlaceType.continent.value,
                GeoPlaceType.country.value,
                GeoPlaceType.region.value,
            ],
        ),
        source_fields=[
            GeoPlaceIndexField.place_name,
            GeoPlaceIndexField.type,
            GeoPlaceIndexField.hierarchies,
        ],
    ):
        feature_id = hit["_id"]
        place_type = GeoPlaceType(hit["_source"]["type"])
        name = hit["_source"]["place_name"]
        hierarchies = [Hierarchy.model_validate(h) for h in hit["_source"]["hierarchies"]]
        cache.add(feature_id, name, place_type, hierarchies)

    return cache


class PlaceCache:
    """A persistent hierarchical place cache loaded from disk if available."""

    # The problem with this current approach is that there are duplicates. We need a way to ensure
    # there are no duplicates. It won't be duplicated by hierachy probably so we can do that.
    _dicts: HierchicalPlaceCache

    _cache_file: Path

    def __init__(self, *, cache_dir: str | Path = "./temp", force_reload: bool = False) -> None:
        # Increment the name of the file when something changes about the format of the storage
        self._cache_file = Path(cache_dir) / "hierarchical_place_cache_v2.json"
        if force_reload or not self._cache_file.exists():
            self._dicts = _populate()
            self._cache_file.parent.mkdir(exist_ok=True)
            with self._cache_file.open("w") as f:
                f.write(self._dicts.to_json(indent=2))
        else:
            with self._cache_file.open() as f:
                self._dicts = HierchicalPlaceCache.from_json(f.read())

    def find_ids(
        self,
        *,
        name: str,
        place_type: GeoPlaceType,
        continent_ids: Collection[str] | None = None,
        country_ids: Collection[str] | None = None,
    ) -> set[str]:
        """Find feature IDs matching the given criteria.

        Args:
            name: Place name to search for
            place_type: Type of place to search for
            continent_ids: Optional filter for specific continents
            country_ids: Optional filter for specific countries

        Returns:
            A set of feature IDs matching the search criteria
        """
        results = self._dicts.find_ids(
            name=name,
            place_type=place_type,
            continent_ids=continent_ids,
            country_ids=country_ids,
        )

        if len(results) == 0 and continent_ids:
            # Due to transcontinental areas we'll allow flexibility if not in the correct continent
            # The hierarchies in the source data for places like Russia are not always complete.
            results = self._dicts.find_ids(
                name=name,
                place_type=place_type,
                country_ids=country_ids,
            )

        return results


## Code for manual testing
# ruff: noqa: ERA001

# cache = _populate()

# place_cache = PlaceCache()

# items = list(place_cache._dicts._name_place_continent_country_to_ids.keys())


# r_items = [t for t in items if t[0] == "Russia"]


# # cache.find_ids(name="France", place_type=GeoPlaceType.country)

# client = create_opensearch_client()

# hits = list(
#     scroll_fetch_all(
#         client,
#         index=GEOPLACE_INDEX_NAME,
#         query=QueryDSL.and_conds(
#             QueryDSL.term(GeoPlaceIndexField.place_name_keyword, "Russia"),
#             QueryDSL.terms(
#                 GeoPlaceIndexField.type,
#                 [
#                     GeoPlaceType.continent.value,
#                     GeoPlaceType.country.value,
#                     GeoPlaceType.region.value,
#                 ],
#             ),
#         ),
#         source_fields=[
#             GeoPlaceIndexField.place_name,
#             GeoPlaceIndexField.type,
#             GeoPlaceIndexField.hierarchies,
#         ],
#     )
# )

# hits
