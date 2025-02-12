import streamlit as st
from e84_geoai_common.debugging import display_geometry
from e84_geoai_common.geometry import geometry_to_geojson, simplify_geometry
from e84_geoai_common.llm.models.claude import BedrockClaudeLLM
from shapely.geometry.base import BaseGeometry
from streamlit_folium import st_folium  # type: ignore[reportUnknownVariableType]

from natural_language_geocoding import extract_geometry_from_text

if "llm" not in st.session_state:
    st.session_state["llm"] = BedrockClaudeLLM()

llm = st.session_state["llm"]


@st.cache_data
def _text_to_geometry(text: str) -> BaseGeometry:
    geometry = extract_geometry_from_text(llm, text)
    return simplify_geometry(geometry)


text = st.text_input("Spatial area", value="within 10 km of the coast of Iberian Peninsula")

geometry = _text_to_geometry(text)
geojson = geometry_to_geojson(geometry)

st.download_button(
    label="Download GeoJSON",
    data=geojson,
    file_name="nl_geocoding.geojson",
    mime="application/json",
)

# call to render Folium map in Streamlit
st_data = st_folium(display_geometry([geometry]), width=1000)
