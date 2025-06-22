import streamlit as st
from e84_geoai_common.debugging import display_geometry
from e84_geoai_common.geometry import geometry_to_geojson
from e84_geoai_common.llm.models import CLAUDE_4_SONNET
from e84_geoai_common.llm.models.claude import BedrockClaudeLLM
from shapely.geometry.base import BaseGeometry
from streamlit_folium import st_folium  # type: ignore[reportUnknownVariableType]

from natural_language_geocoding import extract_geometry_from_text
from natural_language_geocoding.errors import GeocodeError
from natural_language_geocoding.geocode_index.geocode_index_place_lookup import (
    GeocodeIndexPlaceLookup,
)

if "llm" not in st.session_state:
    st.session_state["llm"] = BedrockClaudeLLM(model_id=CLAUDE_4_SONNET)
    st.session_state["place_lookup"] = GeocodeIndexPlaceLookup()

llm = st.session_state["llm"]
place_lookup = st.session_state["place_lookup"]


@st.cache_data
def _text_to_geometry(text: str) -> BaseGeometry | None:
    try:
        return extract_geometry_from_text(llm, text, place_lookup)
    except GeocodeError as e:
        st.error(f"Geocoding error: {e.user_message}")


text = st.text_input("Spatial area", value="within 10 km of the coast of Iberian Peninsula")

geometry = _text_to_geometry(text)
if geometry:
    geojson = geometry_to_geojson(geometry)

    st.download_button(
        label="Download GeoJSON",
        data=geojson,
        file_name="nl_geocoding.geojson",
        mime="application/json",
    )

    # call to render Folium map in Streamlit
    st_data = st_folium(display_geometry([geometry]), width=1000)
