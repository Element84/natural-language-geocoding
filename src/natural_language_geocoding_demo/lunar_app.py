import folium
import streamlit as st
from e84_geoai_common.geometry import geometry_to_geojson
from e84_geoai_common.llm.models import CLAUDE_4_SONNET
from e84_geoai_common.llm.models.claude import BedrockClaudeLLM
from shapely import GeometryCollection
from shapely.geometry.base import BaseGeometry
from streamlit_folium import st_folium  # type: ignore[reportUnknownVariableType]

from natural_language_geocoding import extract_geometry_from_text
from natural_language_geocoding.errors import GeocodeError
from natural_language_geocoding.geocode_index.lunar_place_lookup import LunarPlaceLookup

LUNAR_TILE_URL = (
    "https://trek.nasa.gov/tiles/Moon/EQ/"
    "LRO_WAC_Mosaic_Global_303ppd_v02/1.0.0/default/default028mm/{z}/{y}/{x}.jpg"
)

if "llm" not in st.session_state:
    st.session_state["llm"] = BedrockClaudeLLM(model_id=CLAUDE_4_SONNET)
    st.session_state["place_lookup"] = LunarPlaceLookup()

llm = st.session_state["llm"]
place_lookup = st.session_state["place_lookup"]


def _display_lunar_geometry(geoms: list[BaseGeometry]) -> folium.Map:
    """Create a folium map with NASA lunar tiles and the given geometries."""
    coll = GeometryCollection(geoms)
    hull = coll.convex_hull
    point = hull.centroid
    min_lon, min_lat, max_lon, max_lat = hull.bounds

    m = folium.Map(
        location=[point.y, point.x],
        zoom_start=5,
        min_zoom=1,
        max_zoom=10,
        crs="EPSG4326",
    )
    folium.TileLayer(
        tiles=LUNAR_TILE_URL,
        attr="NASA/GSFC/ASU",
        no_wrap=True,
    ).add_to(m)
    m.fit_bounds([[min_lat, min_lon], [max_lat, max_lon]])

    for geom in geoms:
        g = folium.GeoJson(geom.__geo_interface__)
        g.add_to(m)

    return m


@st.cache_data
def _text_to_geometry(text: str) -> BaseGeometry | None:
    try:
        return extract_geometry_from_text(llm, text, place_lookup, body="moon")
    except GeocodeError as e:
        st.error(f"Geocoding error: {e.user_message}")
        return None


st.title("Lunar Natural Language Geocoding")

text = st.text_input("Lunar location", value="Copernicus crater")

geometry = _text_to_geometry(text)
if geometry:
    geojson = geometry_to_geojson(geometry)

    st.download_button(
        label="Download GeoJSON",
        data=geojson,
        file_name="lunar_geocoding.geojson",
        mime="application/json",
    )

    st_data = st_folium(_display_lunar_geometry([geometry]), width=1000)
