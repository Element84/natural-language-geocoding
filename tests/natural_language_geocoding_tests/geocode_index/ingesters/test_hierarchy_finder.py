from natural_language_geocoding.geocode_index.geoplace import (
    Hierarchy,
)
from natural_language_geocoding.geocode_index.ingesters.hierarchy_finder import (
    _ContinentCountryRegionTracker,  # type: ignore[reportPrivateUsage]
)


def test_continent_country_region_tracker() -> None:
    tracker = _ContinentCountryRegionTracker()

    # Empty tracker should have no hierarchies
    assert tracker.to_hierarchies() == set()

    tracker.add("ne", None, None)
    assert tracker.to_hierarchies() == {Hierarchy(continent_id="ne")}

    # Country within a previous continent is merged
    tracker.add("ne", "usa", None)
    assert tracker.to_hierarchies() == {Hierarchy(continent_id="ne", country_id="usa")}

    # Duplicates are handled
    tracker.add("ne", "usa", None)
    tracker.add("ne", None, None)
    assert tracker.to_hierarchies() == {Hierarchy(continent_id="ne", country_id="usa")}

    # Regions without a continent or country
    tracker.add(None, None, "Mystery Island")
    assert tracker.to_hierarchies() == {
        Hierarchy(continent_id="ne", country_id="usa"),
        Hierarchy(region_id="Mystery Island"),
    }

    # Country without a continent
    tracker.add(None, "Floating Country", None)
    assert tracker.to_hierarchies() == {
        Hierarchy(continent_id="ne", country_id="usa"),
        Hierarchy(region_id="Mystery Island"),
        Hierarchy(country_id="Floating Country"),
    }
    tracker.add(None, "Floating Country", "Floats County")
    assert tracker.to_hierarchies() == {
        Hierarchy(continent_id="ne", country_id="usa"),
        Hierarchy(region_id="Mystery Island"),
        Hierarchy(country_id="Floating Country", region_id="Floats County"),
    }


def test_continent_country_region_tracker_with_hierarches() -> None:
    tracker = _ContinentCountryRegionTracker()

    tracker.add_hierarchies(
        [
            Hierarchy(continent_id="ne", country_id="usa", region_id="Maryland"),
            Hierarchy(continent_id="ne", country_id="usa"),
            Hierarchy(continent_id="ne"),
            Hierarchy(region_id="Mystery Island"),
            Hierarchy(country_id="Floating Country", region_id="Floats County"),
            Hierarchy(borough_id="ignored"),
            Hierarchy(empire_id="ignored"),
        ]
    )
    tracker.add_hierarchies(
        [
            Hierarchy(continent_id="ne", country_id="usa"),
            Hierarchy(continent_id="ne"),
            Hierarchy(continent_id="ne", country_id="usa", region_id="Virginia"),
        ]
    )

    assert tracker.to_hierarchies() == {
        Hierarchy(continent_id="ne", country_id="usa", region_id="Maryland"),
        Hierarchy(continent_id="ne", country_id="usa", region_id="Virginia"),
        Hierarchy(region_id="Mystery Island"),
        Hierarchy(country_id="Floating Country", region_id="Floats County"),
    }
