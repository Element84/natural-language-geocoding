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
    EarthPlace,
    EarthPlaceSource,
    EarthPlaceSourceType,
    EarthPlaceType,
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
    place_type: EarthPlaceType
    alternate_names: list[str] | None = None

    component: CompositionComponent

    def lookup(self, place_lookup: GeocodeIndexPlaceLookup) -> EarthPlace:
        logger.info(
            "Composing together places for %s %s %s", self.id, self.place_name, self.place_type
        )
        composed = self.component.lookup(place_lookup)

        return EarthPlace(
            id=self.id,
            place_name=self.place_name,
            type=self.place_type,
            geom=composed.geom,
            source=EarthPlaceSource(source_type=EarthPlaceSourceType.comp, source_path="composed"),
            hierarchies=composed.hierarchies,
            properties={"sources": [source.model_dump(mode="json") for source in composed.sources]},
            alternate_names=self.alternate_names or [],
        )


compositions = [
    Composition(
        id="comp_atlantic",
        place_name="Atlantic Ocean",
        place_type=EarthPlaceType.ocean,
        component=UnionComponent(
            components=[
                PlaceLookupComponent.with_name_type("North Atlantic Ocean", EarthPlaceType.ocean),
                PlaceLookupComponent.with_name_type("Sargasso Sea", EarthPlaceType.marinearea),
                PlaceLookupComponent.with_name_type("South Atlantic Ocean", EarthPlaceType.ocean),
            ]
        ),
    ),
    Composition(
        id="comp_pacific",
        place_name="Pacific Ocean",
        place_type=EarthPlaceType.ocean,
        component=UnionComponent(
            components=[
                PlaceLookupComponent.with_name_type("North Pacific Ocean", EarthPlaceType.ocean),
                PlaceLookupComponent.with_name_type("South Pacific Ocean", EarthPlaceType.ocean),
            ]
        ),
    ),
    Composition(
        id="comp_mediterranean",
        place_name="Mediterranean Sea",
        place_type=EarthPlaceType.sea,
        component=UnionComponent(
            components=[
                PlaceLookupComponent(
                    request=PlaceSearchRequest(
                        name="Mediterranean Sea",
                        place_type=EarthPlaceType.sea,
                        source_type=EarthPlaceSourceType.ne,
                    ),
                    num_to_combine=2,
                ),
                PlaceLookupComponent.with_name_type("Adriatic Sea", EarthPlaceType.sea),
                PlaceLookupComponent.with_name_type("Aegean Sea", EarthPlaceType.sea),
                PlaceLookupComponent.with_name_type("Tyrrhenian Sea", EarthPlaceType.sea),
                PlaceLookupComponent.with_name_type("Ionian Sea", EarthPlaceType.sea),
                PlaceLookupComponent.with_name_type("Balearic Sea", EarthPlaceType.sea),
                PlaceLookupComponent.with_name_type("Alboran Sea", EarthPlaceType.sea),
                PlaceLookupComponent.with_name_type("Ligurian Sea", EarthPlaceType.sea),
                PlaceLookupComponent.with_name_type("Sea of Crete", EarthPlaceType.sea),
                PlaceLookupComponent.with_name_type("Gulf of Sidra", EarthPlaceType.marinearea),
            ]
        ),
    ),
    Composition(
        id="comp_iberia",
        place_name="Iberian Peninsula",
        place_type=EarthPlaceType.peninsula,
        component=IberianPeninsulaCompositionComponent(),
    ),
    Composition(
        id="comp_north_africa",
        place_name="North Africa",
        place_type=EarthPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Africa",
            countries=["Morocco", "Algeria", "Tunisia", "Libya", "Egypt", "Sudan"],
        ),
    ),
    Composition(
        id="comp_west_africa",
        place_name="West Africa",
        place_type=EarthPlaceType.geoarea,
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
        place_type=EarthPlaceType.geoarea,
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
        place_type=EarthPlaceType.geoarea,
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
        place_type=EarthPlaceType.geoarea,
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
        place_type=EarthPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Asia",
            countries=["China", "Japan", "South Korea", "North Korea", "Mongolia", "Taiwan"],
        ),
    ),
    Composition(
        id="comp_southeast_asia",
        place_name="Southeast Asia",
        place_type=EarthPlaceType.geoarea,
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
        place_type=EarthPlaceType.geoarea,
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
        place_type=EarthPlaceType.geoarea,
        component=ContinentSubregion(
            continent="Asia",
            countries=["Kazakhstan", "Uzbekistan", "Kyrgyzstan", "Tajikistan", "Turkmenistan"],
        ),
    ),
    Composition(
        id="comp_west_asia_middle_east",
        place_name="Middle East",
        alternate_names=["West Asia"],
        place_type=EarthPlaceType.geoarea,
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
        place_type=EarthPlaceType.geoarea,
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
        place_type=EarthPlaceType.geoarea,
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
        place_type=EarthPlaceType.geoarea,
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
        place_type=EarthPlaceType.geoarea,
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
                        PlaceLookupComponent.with_name_type("Europe", EarthPlaceType.continent),
                        PlaceLookupComponent.with_name_type("Russia", EarthPlaceType.country),
                    ]
                ),
            ]
        ),
    ),
    Composition(
        id="comp_balkans",
        place_name="Balkans",
        place_type=EarthPlaceType.geoarea,
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
        place_type=EarthPlaceType.geoarea,
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
        place_type=EarthPlaceType.geoarea,
        component=UnionComponent(
            components=[
                PlaceLookupComponent.with_name_type("Aruba", EarthPlaceType.country),
                PlaceLookupComponent.with_name_type("Antigua and Barbuda", EarthPlaceType.country),
                PlaceLookupComponent.with_name_type("Bonaire", EarthPlaceType.dependency),
                PlaceLookupComponent.with_name_type("Bahamas", EarthPlaceType.country),
                PlaceLookupComponent.with_name_type("Barbados", EarthPlaceType.country),
                PlaceLookupComponent.with_name_type("Cuba", EarthPlaceType.country),
                PlaceLookupComponent.with_name_type("Dominica", EarthPlaceType.country),
                PlaceLookupComponent.with_name_type("Dominican Republic", EarthPlaceType.country),
                PlaceLookupComponent.with_name_type("Grenada", EarthPlaceType.country),
                PlaceLookupComponent.with_name_type("Haiti", EarthPlaceType.country),
                PlaceLookupComponent.with_name_type("Jamaica", EarthPlaceType.country),
                PlaceLookupComponent.with_name_type("Puerto Rico", EarthPlaceType.dependency),
                PlaceLookupComponent.with_name_type(
                    "Saint Kitts and Nevis", EarthPlaceType.country
                ),
                PlaceLookupComponent.with_name_type("Saint Lucia", EarthPlaceType.country),
                PlaceLookupComponent.with_name_type(
                    "Saint Vincent and the Grenadines", EarthPlaceType.country
                ),
                PlaceLookupComponent.with_name_type("Trinidad and Tobago", EarthPlaceType.country),
                PlaceLookupComponent.with_name_type("Curacao", EarthPlaceType.country),
                PlaceLookupComponent.with_name_type("Sint Maarten", EarthPlaceType.country),
                PlaceLookupComponent.with_name_type(
                    "British Virgin Islands", EarthPlaceType.dependency
                ),
                PlaceLookupComponent.with_name_type("Cayman Islands", EarthPlaceType.dependency),
                PlaceLookupComponent.with_name_type("Guadeloupe", EarthPlaceType.region),
                PlaceLookupComponent.with_name_type("Martinique", EarthPlaceType.region),
                PlaceLookupComponent.with_name_type("Montserrat", EarthPlaceType.dependency),
                PlaceLookupComponent.with_name_type("Saint Barthélemy", EarthPlaceType.dependency),
                PlaceLookupComponent.with_name_type("Saint Martin", EarthPlaceType.dependency),
                PlaceLookupComponent.with_name_type(
                    "Turks and Caicos Islands", EarthPlaceType.dependency
                ),
                PlaceLookupComponent.with_name_type(
                    "U.S. Virgin Islands", EarthPlaceType.dependency
                ),
            ]
        ),
    ),
    Composition(
        id="comp_central_america",
        place_name="Central America",
        place_type=EarthPlaceType.geoarea,
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
