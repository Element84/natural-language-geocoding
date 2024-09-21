import os
import gradio as gr  # type: ignore
import folium  # type: ignore
from gradio_folium import Folium  # type: ignore
from e84_geoai_common.llm.core import BedrockClaudeLLM

from natural_language_geocoding import extract_geometry_from_text
from e84_geoai_common.geometry import simplify_geometry
from e84_geoai_common.debugging import display_geometry

CURR_DIR = os.path.dirname(__file__)


llm = BedrockClaudeLLM()


def filter_map(text: str) -> folium.Map:
    geometry = extract_geometry_from_text(llm, text)
    geometry = simplify_geometry(geometry)
    folium_map = display_geometry([geometry])
    return folium_map


# filter_map("within 10 km of the coast of Iberian Peninsula")

with gr.Blocks() as demo:
    text = gr.Textbox(
        value="within 10 km of the coast of Iberian Peninsula",
        label="Query",
    )
    map = Folium(
        # FUTURE if we can get the whole gradio UI to take up a larger height we'll do this.
        # Only accepts int heights but this actually works
        # height="100%",
        height=400,
    )

    # Handle Enter button in text box
    text.submit(  # type: ignore
        fn=filter_map,
        inputs=text,
        outputs=map,
    )

if __name__ == "__main__":
    demo.queue(
        # Disables the API since we don't want anyone using that directly
        api_open=False,
    )
    demo.launch(  # type: ignore
        server_name="0.0.0.0",
        inline=False,  # If True, inline the Gradio interface with the notebook output
    )
