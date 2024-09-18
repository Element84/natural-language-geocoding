import os
import gradio as gr  # type: ignore
import folium  # type: ignore
from gradio_folium import Folium  # type: ignore
from pydantic import BaseModel, ConfigDict
from e84_geoai_common.llm.core import BedrockClaudeLLM, extract_data_from_text

from natural_language_geocoding import NaturalLanguageRequest
from natural_language_geocoding.models import SpatialNode
from e84_geoai_common.geometry import simplify_geometry
from e84_geoai_common.debugging import display_geometry

CURR_DIR = os.path.dirname(__file__)


llm = BedrockClaudeLLM()


class AppState(BaseModel):
    model_config = ConfigDict(
        strict=True, extra="forbid", frozen=True, arbitrary_types_allowed=True
    )
    spatial: SpatialNode


def filter_map(text: str) -> tuple[AppState, folium.Map]:
    spatial_node = extract_data_from_text(llm, NaturalLanguageRequest)
    geometry = spatial_node.to_geometry()
    if geometry is None:
        raise Exception("No geometry. FUTURE handle this better on the UI")
    geometry = simplify_geometry(geometry)
    folium_map = display_geometry([geometry])
    return (AppState(spatial=spatial_node), folium_map)


with gr.Blocks() as demo:
    state = gr.State(None)
    text = gr.Textbox(
        value="within 10 km of the coast of Iberian Peninsula",
        label="Query",
    )
    map = Folium(
        # FUTURE if we can get the whole gradio UI to take up a larger height we'll do this.
        # Only accepts int heights but this actually works
        # height="100%",  # type: ignore
        height=400,
    )

    # Handle Enter button in text box
    text.submit(  # type: ignore
        filter_map,
        [text],
        [state, map],
    )

if __name__ == "__main__":

    # Sets this up so it will work with multiple people. More work will need to be done here to
    # handle a lot of users if necessary.
    # See https://www.gradio.app/guides/setting-up-a-demo-for-maximum-performance
    demo.queue(
        # Disables the API since we don't want anyone using that directly
        api_open=False,
    )
    demo.launch(  # type: ignore
        server_name="0.0.0.0",
        inline=False,  # If True, inline the Gradio interface with the notebook output
    )
