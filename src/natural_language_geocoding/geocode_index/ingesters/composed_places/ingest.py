"""Composed places ingester for geographic regions.

This module defines and ingests composed geographic places that are created by combining
multiple existing places or regions. It handles complex geographic entities like:

- Ocean regions (Atlantic, Pacific, Mediterranean)
- Continental subregions (North Africa, East Asia, Western Europe, etc.)
- Geographic areas formed by unions or intersections of other places

The compositions are defined declaratively and can be ingested into the geocoding index
for use in natural language geocoding queries.
"""

import logging

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
from natural_language_geocoding.geocode_index.index import GeocodeIndex
from natural_language_geocoding.geocode_index.ingesters.composed_places.composers_core import (
    CompositionComponent,
    ContinentSubregion,
    IntersectionComponent,
    PlaceLookupComponent,
    UnionComponent,
)
from natural_language_geocoding.geocode_index.ingesters.composed_places.iberian_peninsula import (
    IberianPeninsulaCompositionComponent,
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
    Composition(
        id="comp_atlantic",
        place_name="Atlantic Ocean",
        place_type=GeoPlaceType.ocean,
        component=UnionComponent(
            components=[
                PlaceLookupComponent.with_name_type("North Atlantic Ocean", GeoPlaceType.ocean),
                PlaceLookupComponent.with_name_type("Sargasso Sea", GeoPlaceType.marinearea),
                PlaceLookupComponent.with_name_type("South Atlantic Ocean", GeoPlaceType.ocean),
            ]
        ),
    ),
    Composition(
        id="comp_pacific",
        place_name="Pacific Ocean",
        place_type=GeoPlaceType.ocean,
        component=UnionComponent(
            components=[
                PlaceLookupComponent.with_name_type("North Pacific Ocean", GeoPlaceType.ocean),
                PlaceLookupComponent.with_name_type("South Pacific Ocean", GeoPlaceType.ocean),
            ]
        ),
    ),
    Composition(
        id="comp_mediterranean",
        place_name="Mediterranean Sea",
        place_type=GeoPlaceType.sea,
        component=UnionComponent(
            components=[
                PlaceLookupComponent(
                    request=PlaceSearchRequest(
                        name="Mediterranean Sea",
                        place_type=GeoPlaceType.sea,
                        source_type=GeoPlaceSourceType.ne,
                    ),
                    num_to_combine=2,
                ),
                PlaceLookupComponent.with_name_type("Adriatic Sea", GeoPlaceType.sea),
                PlaceLookupComponent.with_name_type("Aegean Sea", GeoPlaceType.sea),
                PlaceLookupComponent.with_name_type("Tyrrhenian Sea", GeoPlaceType.sea),
                PlaceLookupComponent.with_name_type("Ionian Sea", GeoPlaceType.sea),
                PlaceLookupComponent.with_name_type("Balearic Sea", GeoPlaceType.sea),
                PlaceLookupComponent.with_name_type("Alboran Sea", GeoPlaceType.sea),
                PlaceLookupComponent.with_name_type("Ligurian Sea", GeoPlaceType.sea),
                PlaceLookupComponent.with_name_type("Sea of Crete", GeoPlaceType.sea),
                PlaceLookupComponent.with_name_type("Gulf of Sidra", GeoPlaceType.marinearea),
            ]
        ),
    ),
    Composition(
        id="comp_iberia",
        place_name="Iberian Peninsula",
        place_type=GeoPlaceType.peninsula,
        component=IberianPeninsulaCompositionComponent(),
    ),
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
                "Benin",
                "Burkina Faso",
                "Cape Verde",
                "Côte d'Ivoire",
                "Gambia",
                "Ghana",
                "Guinea-Bissau",
                "Guinea",
                "Liberia",
                "Mali",
                "Mauritania",
                "Niger",
                "Nigeria",
                "Senegal",
                "Sierra Leone",
                "Togo",
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
                "Cameroon",
                "Central African Republic",
                "Chad",
                "Democratic Republic of Congo",
                "Equatorial Guinea",
                "Gabon",
                "Republic of Congo",
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
                "Angola",
                "Botswana",
                "Comoros",
                "Eswatini",
                "Lesotho",
                "Madagascar",
                "Malawi",
                "Mauritius",
                "Mozambique",
                "Namibia",
                "Seychelles",
                "South Africa",
                "Zambia",
                "Zimbabwe",
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
                "Brunei",
                "Cambodia",
                "Indonesia",
                "Laos",
                "Malaysia",
                "Myanmar",
                "Philippines",
                "Singapore",
                "Thailand",
                "Timor-Leste",
                "Vietnam",
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
                "Afghanistan",
                "Bangladesh",
                "Bhutan",
                "India",
                "Maldives",
                "Nepal",
                "Pakistan",
                "Sri Lanka",
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
                "Bahrain",
                "Iran",
                "Iraq",
                "Israel",
                "Jordan",
                "Kuwait",
                "Lebanon",
                "Oman",
                "Palestine",
                "Qatar",
                "Saudi Arabia",
                "Syria",
                "Turkey",
                "United Arab Emirates",
                "Yemen",
            ],
        ),
    ),
    Composition(
        id="comp_western_europe",
        place_name="Western Europe",
        place_type=GeoPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Europe",
            constrain_to_continent=True,
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
            constrain_to_continent=True,
            countries=[
                "Portugal",
                "Spain",
                "Andorra",
                "Cyprus",
                "Greece",
                "Italy",
                "Malta",
                "San Marino",
                "Vatican City",
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
                        "Belarus",
                        "Bulgaria",
                        "Czech Republic",
                        "Hungary",
                        "Moldova",
                        "Poland",
                        "Romania",
                        "Slovakia",
                        "Ukraine",
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
                "Albania",
                "Bosnia and Herzegovina",
                "Croatia",
                "Kosovo",
                "Montenegro",
                "North Macedonia",
                "Serbia",
                "Slovenia",
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
        component=UnionComponent(
            components=[
                PlaceLookupComponent.with_name_type("Aruba", GeoPlaceType.country),
                PlaceLookupComponent.with_name_type("Antigua and Barbuda", GeoPlaceType.country),
                PlaceLookupComponent.with_name_type("Bonaire", GeoPlaceType.dependency),
                PlaceLookupComponent.with_name_type("Bahamas", GeoPlaceType.country),
                PlaceLookupComponent.with_name_type("Barbados", GeoPlaceType.country),
                PlaceLookupComponent.with_name_type("Cuba", GeoPlaceType.country),
                PlaceLookupComponent.with_name_type("Dominica", GeoPlaceType.country),
                PlaceLookupComponent.with_name_type("Dominican Republic", GeoPlaceType.country),
                PlaceLookupComponent.with_name_type("Grenada", GeoPlaceType.country),
                PlaceLookupComponent.with_name_type("Haiti", GeoPlaceType.country),
                PlaceLookupComponent.with_name_type("Jamaica", GeoPlaceType.country),
                PlaceLookupComponent.with_name_type("Puerto Rico", GeoPlaceType.dependency),
                PlaceLookupComponent.with_name_type("Saint Kitts and Nevis", GeoPlaceType.country),
                PlaceLookupComponent.with_name_type("Saint Lucia", GeoPlaceType.country),
                PlaceLookupComponent.with_name_type(
                    "Saint Vincent and the Grenadines", GeoPlaceType.country
                ),
                PlaceLookupComponent.with_name_type("Trinidad and Tobago", GeoPlaceType.country),
                PlaceLookupComponent.with_name_type("Curacao", GeoPlaceType.country),
                PlaceLookupComponent.with_name_type("Sint Maarten", GeoPlaceType.country),
                PlaceLookupComponent.with_name_type(
                    "British Virgin Islands", GeoPlaceType.dependency
                ),
                PlaceLookupComponent.with_name_type("Cayman Islands", GeoPlaceType.dependency),
                PlaceLookupComponent.with_name_type("Guadeloupe", GeoPlaceType.region),
                PlaceLookupComponent.with_name_type("Martinique", GeoPlaceType.region),
                PlaceLookupComponent.with_name_type("Montserrat", GeoPlaceType.dependency),
                PlaceLookupComponent.with_name_type("Saint Barthélemy", GeoPlaceType.dependency),
                PlaceLookupComponent.with_name_type("Saint Martin", GeoPlaceType.dependency),
                PlaceLookupComponent.with_name_type(
                    "Turks and Caicos Islands", GeoPlaceType.dependency
                ),
                PlaceLookupComponent.with_name_type("U.S. Virgin Islands", GeoPlaceType.dependency),
            ]
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
    """Ingests all of the composed places."""
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


# place_lookup = GeocodeIndexPlaceLookup()
# index = GeocodeIndex()


# composed_places: list[GeoPlace] = []
# failed_compositions: list[Composition] = []

# for comp in compositions:
#     try:
#         composed_places.append(comp.lookup(place_lookup))
#     except Exception as e:
#         print(f"Failed {comp.place_name}", e)
#         failed_compositions.append(comp)


# len(composed_places)

# composed_places[0].display_geometry()
