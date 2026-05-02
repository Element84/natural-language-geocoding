from natural_language_geocoding.geocode_index.geoplace import EarthPlaceType, Hierarchy
from natural_language_geocoding.geocode_index.hierachical_place_cache import HierchicalPlaceCache


def _hier(
    continent_id: str, country_id: str | None = None, region_id: str | None = None
) -> Hierarchy:
    return Hierarchy(continent_id=continent_id, country_id=country_id, region_id=region_id)


_PlaceData = tuple[str, str, EarthPlaceType, list[Hierarchy]]

continent_place_data: list[_PlaceData] = [
    ("na_id", "North America", EarthPlaceType.continent, [_hier("na_id")]),
    ("eu_id", "Europe", EarthPlaceType.continent, [_hier("eu_id")]),
    ("africa_id", "Africa", EarthPlaceType.continent, [_hier("africa_id")]),
]
country_place_data: list[_PlaceData] = [
    # North America
    ("us_id", "United States", EarthPlaceType.country, [_hier("na_id", "us_id")]),
    ("can_id", "Canada", EarthPlaceType.country, [_hier("na_id", "can_id")]),
    ("mex_id", "Mexico", EarthPlaceType.country, [_hier("na_id", "mex_id")]),
    # Europe
    ("uk_id", "United Kingdom", EarthPlaceType.country, [_hier("eu_id", "uk_id")]),
    ("fr_id", "France", EarthPlaceType.country, [_hier("eu_id", "fr_id")]),
    ("de_id", "Germany", EarthPlaceType.country, [_hier("eu_id", "de_id")]),
    ("es_id", "Spain", EarthPlaceType.country, [_hier("eu_id", "es_id")]),
    ("it_id", "Italy", EarthPlaceType.country, [_hier("eu_id", "it_id")]),
    # Africa
    ("za_id", "South Africa", EarthPlaceType.country, [_hier("africa_id", "za_id")]),
    ("eg_id", "Egypt", EarthPlaceType.country, [_hier("africa_id", "eg_id")]),
    ("ng_id", "Nigeria", EarthPlaceType.country, [_hier("africa_id", "ng_id")]),
    ("ke_id", "Kenya", EarthPlaceType.country, [_hier("africa_id", "ke_id")]),
    ("ma_id", "Morocco", EarthPlaceType.country, [_hier("africa_id", "ma_id")]),
]


region_place_data: list[_PlaceData] = [
    # US
    ("maryland_id", "Maryland", EarthPlaceType.region, [_hier("na_id", "us_id", "maryland_id")]),
    ("cali_id", "California", EarthPlaceType.region, [_hier("na_id", "us_id", "cali_id")]),
    # Mexico
    ("jalisco_id", "Jalisco", EarthPlaceType.region, [_hier("na_id", "mex_id", "jalisco_id")]),
    ("cdmx_id", "Mexico City", EarthPlaceType.region, [_hier("na_id", "mex_id", "cdmx_id")]),
    ("nln_id", "Nuevo León", EarthPlaceType.region, [_hier("na_id", "mex_id", "nln_id")]),
    # Fake region that's in mexico and the us
    (
        "desert_id",
        "Desertish",
        EarthPlaceType.region,
        [_hier("na_id", "us_id", "desert_id"), _hier("na_id", "mex_id", "desert_id")],
    ),
    # UK
    ("england_id", "England", EarthPlaceType.region, [_hier("eu_id", "uk_id", "england_id")]),
    ("scotland_id", "Scotland", EarthPlaceType.region, [_hier("eu_id", "uk_id", "scotland_id")]),
    # Fake duplicate in the UK
    (
        "maryland_uk_id",
        "Maryland",
        EarthPlaceType.region,
        [_hier("eu_id", "uk_id", "maryland_uk_id")],
    ),
    # Germany
    ("bavaria_id", "Bavaria", EarthPlaceType.region, [_hier("eu_id", "de_id", "bavaria_id")]),
    (
        "nrw_id",
        "North Rhine-Westphalia",
        EarthPlaceType.region,
        [_hier("eu_id", "de_id", "nrw_id")],
    ),
    # Spain
    ("catalonia_id", "Catalonia", EarthPlaceType.region, [_hier("eu_id", "es_id", "catalonia_id")]),
    ("andalusia_id", "Andalusia", EarthPlaceType.region, [_hier("eu_id", "es_id", "andalusia_id")]),
]

cache = HierchicalPlaceCache()

for fid, name, place_type, hierarchies in [
    *continent_place_data,
    *country_place_data,
    *region_place_data,
]:
    cache.add(fid, name, place_type, hierarchies)


def test_find_by_name_type() -> None:
    # Find None by name
    assert cache.find_ids(name="fake", place_type=EarthPlaceType.region) == set()
    # Find None by place type
    assert cache.find_ids(name="United States", place_type=EarthPlaceType.region) == set()

    # Find one continent
    assert cache.find_ids(name="North America", place_type=EarthPlaceType.continent) == {"na_id"}
    # Find one country
    assert cache.find_ids(name="United States", place_type=EarthPlaceType.country) == {"us_id"}

    # Find multiple regions
    assert cache.find_ids(name="Maryland", place_type=EarthPlaceType.region) == {
        "maryland_id",
        "maryland_uk_id",
    }


def test_find_by_name_type_continent() -> None:
    # Find None
    assert (
        cache.find_ids(
            name="United States", place_type=EarthPlaceType.country, continent_ids=["eu_id"]
        )
        == set()
    )
    # Find one continent
    assert cache.find_ids(
        name="North America", place_type=EarthPlaceType.continent, continent_ids=["na_id"]
    ) == {"na_id"}
    # Find one country
    assert cache.find_ids(
        name="United States", place_type=EarthPlaceType.country, continent_ids=["na_id"]
    ) == {"us_id"}

    # Find regions
    assert cache.find_ids(
        name="Maryland", place_type=EarthPlaceType.region, continent_ids=["na_id"]
    ) == {"maryland_id"}
    assert cache.find_ids(
        name="Maryland", place_type=EarthPlaceType.region, continent_ids=["eu_id"]
    ) == {"maryland_uk_id"}
    assert cache.find_ids(
        name="Maryland", place_type=EarthPlaceType.region, continent_ids=["eu_id", "na_id"]
    ) == {"maryland_uk_id", "maryland_id"}


def test_find_by_name_type_country() -> None:
    # Find None
    assert (
        cache.find_ids(name="Maryland", place_type=EarthPlaceType.region, country_ids=["mex_id"])
        == set()
    )
    # Find one country
    assert cache.find_ids(
        name="United States", place_type=EarthPlaceType.country, country_ids=["us_id"]
    ) == {"us_id"}

    # Find regions
    assert cache.find_ids(
        name="Maryland", place_type=EarthPlaceType.region, country_ids=["us_id"]
    ) == {"maryland_id"}
    assert cache.find_ids(
        name="Maryland", place_type=EarthPlaceType.region, country_ids=["uk_id"]
    ) == {"maryland_uk_id"}
    assert cache.find_ids(
        name="Maryland", place_type=EarthPlaceType.region, country_ids=["us_id", "uk_id"]
    ) == {"maryland_uk_id", "maryland_id"}


def test_find_by_name_type_continent_country() -> None:
    # Find None
    assert (
        cache.find_ids(
            name="Maryland",
            place_type=EarthPlaceType.region,
            continent_ids=["na_id"],
            country_ids=["mex_id"],
        )
        == set()
    )
    # Find one country
    assert cache.find_ids(
        name="United States",
        place_type=EarthPlaceType.country,
        continent_ids=["na_id"],
        country_ids=["us_id"],
    ) == {"us_id"}

    # Find regions
    assert cache.find_ids(
        name="Maryland",
        place_type=EarthPlaceType.region,
        continent_ids=["na_id"],
        country_ids=["us_id"],
    ) == {"maryland_id"}
    assert cache.find_ids(
        name="Maryland",
        place_type=EarthPlaceType.region,
        continent_ids=["eu_id"],
        country_ids=["uk_id"],
    ) == {"maryland_uk_id"}
    assert cache.find_ids(
        name="Maryland",
        place_type=EarthPlaceType.region,
        continent_ids=["na_id", "eu_id"],
        country_ids=["us_id", "uk_id"],
    ) == {"maryland_uk_id", "maryland_id"}
