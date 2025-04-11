"""TODO docs."""

import logging

from e84_geoai_common.debugging import display_geometry
from pydantic import BaseModel, ConfigDict

from natural_language_geocoding.geocode_index.geocode_index_place_lookup import (
    GeocodeIndexPlaceLookup,
)
from natural_language_geocoding.geocode_index.geoplace import (
    GeoPlace,
    GeoPlaceSource,
    GeoPlaceSourceType,
    GeoPlaceType,
)
from natural_language_geocoding.geocode_index.index import GeocodeIndex, print_places_with_names
from natural_language_geocoding.geocode_index.ingesters.composed_places.composers_core import (
    CompositionComponent,
    ContinentSubregion,
    IntersectionComponent,
    PlaceLookupComponent,
    UnionComponent,
)
from natural_language_geocoding.place_lookup import PlaceSearchRequest

logger = logging.getLogger(__name__)


class Composition(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    id: str
    place_name: str
    place_type: GeoPlaceType
    alternate_names: list[str] | None = None

    component: CompositionComponent

    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> GeoPlace:
        logger.info(
            "Composing together places for %s %s %s", self.id, self.place_name, self.place_type
        )
        composed = self.component.lookup(place_lookup)

        return GeoPlace(
            id=self.id,
            place_name=self.place_name,
            type=self.place_type,
            geom=composed.geom,
            source=GeoPlaceSource(source_type=GeoPlaceSourceType.comp, source_path="composed"),
            hierarchies=composed.hierarchies,
            properties={"sources": [source.model_dump(mode="json") for source in composed.sources]},
            alternate_names=self.alternate_names or [],
        )


compositions = [
    # TODO temporarily commenting out these places that work.
    # Composition(
    #     id="comp_atlantic",
    #     place_name="Atlantic Ocean",
    #     place_type=GeoPlaceType.ocean,
    #     component=UnionComponent(
    #         components=[
    #             PlaceLookupComponent.with_name_type("North Atlantic Ocean", GeoPlaceType.ocean),
    #             PlaceLookupComponent.with_name_type("Sargasso Sea", GeoPlaceType.marinearea),
    #             PlaceLookupComponent.with_name_type("South Atlantic Ocean", GeoPlaceType.ocean),
    #         ]
    #     ),
    # ),
    # Composition(
    #     id="comp_pacific",
    #     place_name="Pacific Ocean",
    #     place_type=GeoPlaceType.ocean,
    #     component=UnionComponent(
    #         components=[
    #             PlaceLookupComponent.with_name_type("North Pacific Ocean", GeoPlaceType.ocean),
    #             PlaceLookupComponent.with_name_type("South Pacific Ocean", GeoPlaceType.ocean),
    #         ]
    #     ),
    # ),
    # Composition(
    #     id="comp_mediterranean",
    #     place_name="Mediterranean Sea",
    #     place_type=GeoPlaceType.sea,
    #     component=UnionComponent(
    #         components=[
    #             PlaceLookupComponent(
    #                 request=PlaceSearchRequest(
    #                     name="Mediterranean Sea",
    #                     place_type=GeoPlaceType.sea,
    #                     source_type=GeoPlaceSourceType.ne,
    #                 ),
    #                 num_to_combine=2,
    #             ),
    #             PlaceLookupComponent.with_name_type("Adriatic Sea", GeoPlaceType.sea),
    #             PlaceLookupComponent.with_name_type("Aegean Sea", GeoPlaceType.sea),
    #             PlaceLookupComponent.with_name_type("Tyrrhenian Sea", GeoPlaceType.sea),
    #             PlaceLookupComponent.with_name_type("Ionian Sea", GeoPlaceType.sea),
    #             PlaceLookupComponent.with_name_type("Balearic Sea", GeoPlaceType.sea),
    #             PlaceLookupComponent.with_name_type("Alboran Sea", GeoPlaceType.sea),
    #             PlaceLookupComponent.with_name_type("Ligurian Sea", GeoPlaceType.sea),
    #             PlaceLookupComponent.with_name_type("Sea of Crete", GeoPlaceType.sea),
    #             PlaceLookupComponent.with_name_type("Gulf of Sidra", GeoPlaceType.marinearea),
    #         ]
    #     ),
    # ),
    # Composition(
    #     id="comp_iberia",
    #     place_name="Iberian Peninsula",
    #     place_type=GeoPlaceType.peninsula,
    #     component=IberianPeninsulaCompositionComponent(),
    # ),
    Composition(
        id="comp_north_africa",
        place_name="North Africa",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Africa",
            countries=["Morocco", "Algeria", "Tunisia", "Libya", "Egypt", "Sudan"],
        ),
    ),
    Composition(
        id="comp_west_africa",
        place_name="West Africa",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Africa",
            countries=[
                "Nigeria",
                "Ghana",
                "Côte d'Ivoire",
                "Senegal",
                "Mali",
                "Burkina Faso",
                "Niger",
                "Benin",
                "Togo",
                "Guinea",
                "Sierra Leone",
                "Liberia",
                "Gambia",
                "Guinea-Bissau",
                "Cape Verde",
                "Mauritania",
                "Western Sahara",
            ],
        ),
    ),
    Composition(
        id="comp_east_africa",
        place_name="East Africa",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Africa",
            countries=[
                "Ethiopia",
                "Kenya",
                "Tanzania",
                "Uganda",
                "Rwanda",
                "Burundi",
                "Djibouti",
                "Eritrea",
                "Somalia",
                "Somaliland",
                "South Sudan",
            ],
        ),
    ),
    Composition(
        id="comp_central_africa",
        place_name="Central Africa",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Africa",
            countries=[
                "Democratic Republic of Congo",
                "Cameroon",
                "Central African Republic",
                "Chad",
                "Republic of Congo",
                "Gabon",
                "Equatorial Guinea",
                "São Tomé and Príncipe",
            ],
        ),
    ),
    Composition(
        id="comp_southern_africa",
        place_name="Southern Africa",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Africa",
            countries=[
                "South Africa",
                "Namibia",
                "Botswana",
                "Zimbabwe",
                "Mozambique",
                "Zambia",
                "Malawi",
                "Angola",
                "Lesotho",
                "Eswatini",
                "Madagascar",
                "Comoros",
                "Mauritius",
                "Seychelles",
            ],
        ),
    ),
    Composition(
        id="comp_east_asia",
        place_name="East Asia",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Asia",
            countries=["China", "Japan", "South Korea", "North Korea", "Mongolia", "Taiwan"],
        ),
    ),
    Composition(
        id="comp_southeast_asia",
        place_name="Southeast Asia",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Asia",
            countries=[
                "Indonesia",
                "Thailand",
                "Philippines",
                "Vietnam",
                "Myanmar",
                "Malaysia",
                "Singapore",
                "Cambodia",
                "Laos",
                "Brunei",
                "Timor-Leste",
            ],
        ),
    ),
    Composition(
        id="comp_south_asia",
        place_name="South Asia",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Asia",
            countries=[
                "India",
                "Pakistan",
                "Bangladesh",
                "Afghanistan",
                "Sri Lanka",
                "Nepal",
                "Bhutan",
                "Maldives",
            ],
        ),
    ),
    Composition(
        id="comp_central_asia",
        place_name="Central Asia",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Asia",
            countries=["Kazakhstan", "Uzbekistan", "Kyrgyzstan", "Tajikistan", "Turkmenistan"],
        ),
    ),
    Composition(
        id="comp_west_asia_middle_east",
        place_name="Middle East",
        alternate_names=["West Asia"],
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Asia",
            countries=[
                "Saudi Arabia",
                "Iran",
                "Turkey",
                "Iraq",
                "Israel",
                "Syria",
                "United Arab Emirates",
                "Lebanon",
                "Jordan",
                "Kuwait",
                "Oman",
                "Qatar",
                "Bahrain",
                "Yemen",
                "Palestine",
            ],
        ),
    ),
    Composition(
        id="comp_western_europe",
        place_name="Western Europe",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Europe",
            countries=[
                "Austria",
                "Belgium",
                "France",
                "Germany",
                "Liechtenstein",
                "Luxembourg",
                "Monaco",
                "Netherlands",
                "Switzerland",
            ],
        ),
    ),
    Composition(
        id="comp_northern_europe",
        place_name="Northern Europe",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Europe",
            countries=[
                "Denmark",
                "Estonia",
                "Finland",
                "Iceland",
                "Latvia",
                "Lithuania",
                "Norway",
                "Sweden",
            ],
        ),
    ),
    Composition(
        id="comp_southern_europe",
        place_name="Southern Europe",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Europe",
            countries=[
                "Italy",
                "Spain",
                "Portugal",
                "Greece",
                "Malta",
                "Cyprus",
                "San Marino",
                "Vatican City",
                "Andorra",
            ],
        ),
    ),
    Composition(
        id="comp_eastern_europe",
        place_name="Eastern Europe",
        place_type=GeoPlaceType.geoarea,
        component=UnionComponent(
            components=[
                ContinentSubregion(
                    continent="Europe",
                    countries=[
                        "Poland",
                        "Ukraine",
                        "Romania",
                        "Czech Republic",
                        "Hungary",
                        "Belarus",
                        "Bulgaria",
                        "Slovakia",
                        "Moldova",
                    ],
                ),
                # Plus the part of Russia in Europe
                IntersectionComponent(
                    components=[
                        PlaceLookupComponent.with_name_type("Europe", GeoPlaceType.continent),
                        PlaceLookupComponent.with_name_type("Russia", GeoPlaceType.country),
                    ]
                ),
            ]
        ),
    ),
    Composition(
        id="comp_balkans",
        place_name="Balkans",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Europe",
            countries=[
                "Croatia",
                "Serbia",
                "Bosnia and Herzegovina",
                "Albania",
                "North Macedonia",
                "Montenegro",
                "Slovenia",
                "Kosovo",
            ],
        ),
    ),
    Composition(
        id="comp_british_isles",
        place_name="British Isles",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Europe",
            countries=[
                "United Kingdom",
                "Ireland",
            ],
        ),
    ),
    Composition(
        id="comp_caribbean",
        place_name="Caribbean",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="North America",
            countries=[
                "Cuba",
                "Jamaica",
                "Haiti",
                "Dominican Republic",
                "Puerto Rico",
                "Bahamas",
                "Trinidad and Tobago",
                "Barbados",
                "Saint Lucia",
                "Grenada",
                "Saint Vincent and the Grenadines",
                "Antigua and Barbuda",
                "Dominica",
                "Saint Kitts and Nevis",
            ],
        ),
    ),
    Composition(
        id="comp_central_america",
        place_name="Central America",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="North America",
            countries=[
                "Belize",
                "Costa Rica",
                "El Salvador",
                "Guatemala",
                "Honduras",
                "Nicaragua",
                "Panama",
            ],
        ),
    ),
]


def ingest_compositions() -> None:
    """TODO docs."""
    place_lookup = GeocodeIndexPlaceLookup()
    logger.info("Generating combined compositions")
    places = [comp.lookup(place_lookup) for comp in compositions]
    index = GeocodeIndex()
    logger.info("Indexing compositions")
    index.bulk_index(places)


if __name__ == "__main__" and "get_ipython" not in globals():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    ingest_compositions()


#################################
# Code for debugging
# ruff: noqa: ERA001


place_lookup = GeocodeIndexPlaceLookup()
index = GeocodeIndex()


composed_places: list[GeoPlace] = []
failed_compositions: list[Composition] = []

for comp in compositions:
    try:
        composed_places.append(comp.lookup(place_lookup))
    except Exception as e:  # noqa: BLE001
        print(f"Failed {comp.place_name}", e)  # noqa: T201
        failed_compositions.append(comp)


len(composed_places)

# len(places)
# Missing part of somalia

# North Africa
composed_places[0].display_geometry()

# West Africa
composed_places[1].display_geometry()

# east africa
composed_places[2].display_geometry()


# Central africa
composed_places[3].display_geometry()
# Southern Africa
composed_places[4].display_geometry()

# All of Africa
display_geometry([p.geom for p in composed_places[0:5]])


# East Asia
composed_places[5].display_geometry()

# southEast Asia
composed_places[6].display_geometry()

# South asia
composed_places[7].display_geometry()

# Central Asia
composed_places[8].display_geometry()

# Middle East
composed_places[9].display_geometry()

# Most of Asia
display_geometry([p.geom for p in composed_places[5:10]])

print(composed_places[10].place_name)  # noqa: T201

# Western Europe TODO needs fixing
composed_places[10].display_geometry()


resp = place_lookup.search_for_places(
    PlaceSearchRequest(name="Sahara", place_type=GeoPlaceType.country, in_continent="Africa"),
    limit=50,
)

sahara_countries = [p for p in resp.places if p.type == GeoPlaceType.country]

print_places_with_names(index, sahara_countries)
sahara_countries[0].display_geometry()


resp.places[3].display_geometry()


# places[0].display_geometry()
# places[1].display_geometry()
# places[2].display_geometry()
# places[3].display_geometry()
