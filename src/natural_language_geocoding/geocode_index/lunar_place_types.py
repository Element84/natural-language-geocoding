"""Defines place types for lunar features."""

from enum import Enum


class LunarPlaceType(Enum):
    """The set of different place types for lunar features.

    Based on the USGS Planetary Nomenclature feature types for the Moon.
    """

    crater = "crater"
    satellite_feature = "satellite_feature"
    mare = "mare"
    mons = "mons"
    rima = "rima"
    dorsum = "dorsum"
    lacus = "lacus"
    catena = "catena"
    vallis = "vallis"
    sinus = "sinus"
    promontorium = "promontorium"
    rupes = "rupes"
    landing_site = "landing_site"
    palus = "palus"
    oceanus = "oceanus"
    planitia = "planitia"
    albedo_feature = "albedo_feature"
    astronaut_named = "astronaut_named"


# The sort order for search results by lunar place type. Primary named features are ranked higher
# than satellite features.
DEFAULT_LUNAR_PLACE_TYPE_SORT_ORDER: list[LunarPlaceType | str] = [
    LunarPlaceType.mare,
    LunarPlaceType.oceanus,
    LunarPlaceType.crater,
    LunarPlaceType.mons,
    LunarPlaceType.vallis,
    LunarPlaceType.rima,
    LunarPlaceType.rupes,
    LunarPlaceType.landing_site,
    LunarPlaceType.lacus,
    LunarPlaceType.sinus,
    LunarPlaceType.palus,
    LunarPlaceType.dorsum,
    LunarPlaceType.catena,
    LunarPlaceType.promontorium,
    LunarPlaceType.planitia,
    LunarPlaceType.albedo_feature,
    LunarPlaceType.astronaut_named,
    LunarPlaceType.satellite_feature,
]


# Mapping from shapefile type strings to LunarPlaceType values
SHAPEFILE_TYPE_TO_LUNAR_PLACE_TYPE: dict[str, LunarPlaceType] = {
    "Crater, craters": LunarPlaceType.crater,
    "Satellite Feature": LunarPlaceType.satellite_feature,
    "Mare, maria": LunarPlaceType.mare,
    "Mons, montes": LunarPlaceType.mons,
    "Rima, rimae": LunarPlaceType.rima,
    "Dorsum, dorsa": LunarPlaceType.dorsum,
    "Lacus, lacūs": LunarPlaceType.lacus,
    "Catena, catenae": LunarPlaceType.catena,
    "Vallis, valles": LunarPlaceType.vallis,
    "Sinus, sinūs": LunarPlaceType.sinus,
    "Promontorium, promontoria": LunarPlaceType.promontorium,
    "Rupes, rupēs": LunarPlaceType.rupes,
    "Statio": LunarPlaceType.landing_site,
    "Palus, paludes": LunarPlaceType.palus,
    "Oceanus, oceani": LunarPlaceType.oceanus,
    "Planitia, planitiae": LunarPlaceType.planitia,
    "Albedo Feature": LunarPlaceType.albedo_feature,
    "Astronaut-named features": LunarPlaceType.astronaut_named,
}
