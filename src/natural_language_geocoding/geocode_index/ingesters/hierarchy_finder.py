"""Contains a function and related code for determining the hierarchy of places.

This is useful for ingesting places other than Who's on First places that come with hierarchies
predefined.
"""

import rich
from rich.tree import Tree
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.geocode_index.geoplace import (
    GeoPlaceType,
    Hierarchy,
)
from natural_language_geocoding.geocode_index.index import (
    GeocodeIndex,
    GeoPlaceIndexField,
    SearchRequest,
)
from natural_language_geocoding.geocode_index.opensearch_utils import QueryDSL


class _ContinentCountryRegionTracker:
    """Takes existing hierarchies and disambiguates them to either continent, country, or region.

    Internally it maintains a tree structure of continents, countries, regions. As more information
    is added the tree is updated.
    """

    continent_to_country_to_region: dict[str | None, dict[str | None, set[str]]]

    def __init__(self) -> None:
        self.continent_to_country_to_region = {}

    def add(self, continent_id: str | None, country_id: str | None, region_id: str | None) -> None:
        """Adds continent, country, and/or region to the set being tracked."""
        if continent_id not in self.continent_to_country_to_region:
            self.continent_to_country_to_region[continent_id] = {}

        if country_id or region_id:
            country_map = self.continent_to_country_to_region[continent_id]

            if country_id not in country_map:
                country_map[country_id] = set()
            region_ids = country_map[country_id]

            if region_id:
                region_ids.add(region_id)

    def add_hierarchies(self, hierarchies: list[Hierarchy]) -> None:
        """Adds the set of hierarchies to the tree.

        Anything hierarchy identifiers other than continent, country, or region is ignored.
        """
        for h in hierarchies:
            if h.continent_id or h.country_id or h.region_id:
                self.add(h.continent_id, h.country_id, h.region_id)

    def display(self) -> None:
        """Displays the internal tree for debugging."""
        tree = Tree("_ContinentCountryRegionTracker", hide_root=True)
        for continent, country_map in self.continent_to_country_to_region.items():
            cont_node = tree.add(f"Continent {continent}")
            for country, regions in country_map.items():
                country_node = cont_node.add(f"Country {country}")
                if len(regions) == 0:
                    country_node.add("No regions")
                else:
                    for region in regions:
                        country_node.add(region)
        rich.print(tree)

    def to_hierarchies(self) -> set[Hierarchy]:
        """Converts the set of continents, countries, and regions into a set of hierarchies."""
        hierarchies: set[Hierarchy] = set()
        for continent, country_map in self.continent_to_country_to_region.items():
            if len(country_map) == 0:
                hierarchies.add(Hierarchy(continent_id=continent))
            else:
                for country, regions in country_map.items():
                    if len(regions) == 0:
                        hierarchies.add(Hierarchy(continent_id=continent, country_id=country))
                    else:
                        for region in regions:
                            hierarchies.add(
                                Hierarchy(
                                    continent_id=continent, country_id=country, region_id=region
                                )
                            )
        return hierarchies


def get_hierarchies(
    index: GeocodeIndex,
    geom: BaseGeometry,
) -> set[Hierarchy]:
    """Finds the parents of a place using spatial location and returns them as a set of Hierarchies.

    An existing set of places must already be indexed in order for this to be useful. Only finds
    parents at the continent, country, or region level.
    """
    max_parents = 2000
    request = SearchRequest(
        search_type="query_then_fetch",
        size=max_parents,
        query=QueryDSL.and_conds(
            QueryDSL.terms(
                GeoPlaceIndexField.type,
                [
                    # Note if adding more types here in the future they also need to be indexed
                    # spatially. The current implementation only indexes shapes for these areas.
                    GeoPlaceType.continent.value,
                    GeoPlaceType.country.value,
                    GeoPlaceType.region.value,
                ],
            ),
            QueryDSL.geo_shape(GeoPlaceIndexField.geom_spatial, geom),
        ),
    )
    resp = index.search(request)
    if resp.hits > max_parents:
        raise Exception(f"Found more than {max_parents}")

    tracker = _ContinentCountryRegionTracker()
    for parent in resp.places:
        tracker.add_hierarchies(parent.self_as_hierarchies())
    return tracker.to_hierarchies()
