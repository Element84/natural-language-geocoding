"""Defines examples and methods for evaluating natural language geocoding."""

import logging
from pathlib import Path

from e84_geoai_common.llm.core import LLM
from e84_geoai_common.llm.models import CLAUDE_4_SONNET, BedrockClaudeLLM

from natural_language_geocoding import parse_spatial_node_from_text
from natural_language_geocoding.eval.evaluation_core import Evaluator, ExampleEval
from natural_language_geocoding.geocode_index.geoplace import GeoPlaceType
from natural_language_geocoding.models import (
    AnySpatialNodeType,
    Between,
    BorderBetween,
    BorderOf,
    Buffer,
    CoastOf,
    Difference,
    DirectionalConstraint,
    Intersection,
    NamedPlace,
    OffTheCoastOf,
    Union,
)

_TEMP_DIR = Path("temp")

# FUTURE Add failure case evaluations. The LLM should be able to detect that it can't return an
# area and return an error.

NAMED_PLACE_EXAMPLES: list[ExampleEval[AnySpatialNodeType]] = [
    ExampleEval(
        user_text="Paris",
        description="Simple city lookup",
        expected_node=NamedPlace(
            name="Paris",
            type=GeoPlaceType.locality,
            in_continent="Europe",
            in_country="France",
        ),
    ),
    ExampleEval(
        user_text="France",
        description="Simple country lookup",
        expected_node=NamedPlace(
            name="France",
            type=GeoPlaceType.country,
            in_continent="Europe",
        ),
    ),
    ExampleEval(
        user_text="Paris, France",
        description="City scoped to a country",
        expected_node=NamedPlace(
            name="Paris",
            type=GeoPlaceType.locality,
            in_continent="Europe",
            in_country="France",
        ),
    ),
    ExampleEval(
        user_text="United States",
        description="USA lookup",
        expected_node=NamedPlace(
            name="United States",
            type=GeoPlaceType.country,
            in_continent="North America",
        ),
    ),
    ExampleEval(
        user_text="Annapolis, Maryland",
        description="City scoped to a US State",
        expected_node=NamedPlace(
            name="Annapolis",
            type=GeoPlaceType.locality,
            in_continent="North America",
            in_country="United States",
            in_region="Maryland",
        ),
    ),
    ExampleEval(
        user_text="Mississippi",
        description="Ambiguous state or river defaults to state",
        expected_node=NamedPlace(
            name="Mississippi",
            type=GeoPlaceType.region,
            in_continent="North America",
            in_country="United States",
        ),
    ),
    ExampleEval(
        user_text="Mississippi River",
        description="River",
        expected_node=NamedPlace(
            name="Mississippi River",
            type=GeoPlaceType.river,
            in_continent="North America",
            in_country="United States",
        ),
    ),
    ExampleEval(
        user_text="Maui",
        description="Island",
        expected_node=NamedPlace(
            name="Maui",
            type=GeoPlaceType.island,
            in_continent="North America",
            in_country="United States",
            in_region="Hawaii",
        ),
    ),
    ExampleEval(
        user_text="The mediterranean",
        description="A sea",
        expected_node=NamedPlace(name="Mediterranean Sea", type=GeoPlaceType.sea),
    ),
    ExampleEval(
        user_text="West Africa",
        description="Geoarea",
        expected_node=NamedPlace(name="West Africa", type=GeoPlaceType.geoarea),
    ),
]

FEATURE_EXAMPLES: list[ExampleEval[AnySpatialNodeType]] = [
    ExampleEval(
        user_text="the coast of Maryland",
        description="Coastline of a US State",
        expected_node=CoastOf(
            child_node=NamedPlace(
                name="Maryland",
                type=GeoPlaceType.region,
                in_continent="North America",
                in_country="United States",
            )
        ),
    ),
    ExampleEval(
        user_text="3km off the coast of Maryland",
        description="Off the coast of a US state",
        expected_node=OffTheCoastOf(
            child_node=NamedPlace(
                name="Maryland",
                type=GeoPlaceType.region,
                in_continent="North America",
                in_country="United States",
            ),
            distance=3,
            distance_unit="kilometers",
        ),
    ),
    ExampleEval(
        user_text="West of London",
        description="Simple directional constraint",
        expected_node=DirectionalConstraint(
            child_node=NamedPlace(
                name="London",
                type=GeoPlaceType.locality,
                in_continent="Europe",
                in_country="United Kingdom",
            ),
            direction="west",
        ),
    ),
    ExampleEval(
        user_text="Border between France and Spain",
        description="Simple border between two areas",
        expected_node=BorderBetween(
            child_node_1=NamedPlace(
                name="France", type=GeoPlaceType.country, in_continent="Europe"
            ),
            child_node_2=NamedPlace(name="Spain", type=GeoPlaceType.country, in_continent="Europe"),
        ),
    ),
    ExampleEval(
        user_text="Border between Oman and Yemen",
        description="Simple border between two areas that may miss continent",
        expected_node=BorderBetween(
            child_node_1=NamedPlace(name="Oman", type=GeoPlaceType.country, in_continent="Asia"),
            child_node_2=NamedPlace(name="Yemen", type=GeoPlaceType.country, in_continent="Asia"),
        ),
    ),
    ExampleEval(
        user_text="Border of France",
        description="Simple border extraction",
        expected_node=BorderOf(
            child_node=NamedPlace(name="France", type=GeoPlaceType.country, in_continent="Europe")
        ),
    ),
    ExampleEval(
        user_text="Within 10 miles of France",
        description="Buffer of an area",
        expected_node=Buffer(
            child_node=NamedPlace(name="France", type=GeoPlaceType.country, in_continent="Europe"),
            distance=10,
            distance_unit="miles",
        ),
    ),
    ExampleEval(
        user_text="Any area in France within 100 miles of London",
        description="Intersection in combination with other areas",
        expected_node=Intersection(
            child_nodes=[
                NamedPlace(name="France", type=GeoPlaceType.country, in_continent="Europe"),
                Buffer(
                    child_node=NamedPlace(
                        name="London",
                        type=GeoPlaceType.locality,
                        in_continent="Europe",
                        in_country="United Kingdom",
                    ),
                    distance=100,
                    distance_unit="miles",
                ),
            ]
        ),
    ),
    ExampleEval(
        user_text="France and Spain",
        description="Simple union",
        expected_node=Union(
            child_nodes=[
                NamedPlace(name="France", type=GeoPlaceType.country, in_continent="Europe"),
                NamedPlace(name="Spain", type=GeoPlaceType.country, in_continent="Europe"),
            ]
        ),
    ),
    ExampleEval(
        user_text="France except for Paris",
        description="Simple Difference",
        expected_node=Difference(
            child_node_1=NamedPlace(
                name="France", type=GeoPlaceType.country, in_continent="Europe"
            ),
            child_node_2=NamedPlace(
                name="Paris",
                type=GeoPlaceType.locality,
                in_continent="Europe",
                in_country="France",
            ),
        ),
    ),
    ExampleEval(
        user_text="Between Baltimore and Washington DC",
        description="Simple Between",
        expected_node=Between(
            child_node_1=NamedPlace(
                name="Baltimore",
                type=GeoPlaceType.locality,
                in_continent="North America",
                in_country="United States",
                in_region="Maryland",
            ),
            child_node_2=NamedPlace(
                name="Washington",
                type=GeoPlaceType.locality,
                in_continent="North America",
                in_country="United States",
            ),
        ),
    ),
    ExampleEval(
        user_text="""
            within 50km of the port of Shanghai's shipping lanes and South China Sea
            territorial waters
        """.strip(),
        description="Union of an area on the water.",
        expected_node=Buffer(
            distance=50,
            distance_unit="kilometers",
            child_node=Union(
                child_nodes=[
                    NamedPlace(
                        name="Shanghai",
                        type=GeoPlaceType.locality,
                        in_country="China",
                        in_continent="Asia",
                    ),
                    NamedPlace(name="South China Sea", type=GeoPlaceType.sea),
                ]
            ),
        ),
    ),
]

ALL_EXAMPLES = [*NAMED_PLACE_EXAMPLES, *FEATURE_EXAMPLES]


class ParseSpatialNodeEvaluator(Evaluator[AnySpatialNodeType]):
    def parse(self, llm: LLM, user_text: str) -> AnySpatialNodeType:
        return parse_spatial_node_from_text(llm, user_text).root


if __name__ == "__main__" and "get_ipython" not in globals():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    llm = BedrockClaudeLLM(model_id=CLAUDE_4_SONNET)
    evaluator = ParseSpatialNodeEvaluator()
    full_eval = evaluator.evaluate_examples(llm, ALL_EXAMPLES)
    print(full_eval.to_markdown())  # noqa: T201

    full_eval.save()


#################
# code for manual testing
# ruff: noqa: ERA001

# llm = BedrockClaudeLLM(model_id=CLAUDE_4_SONNET)
# evaluator = ParseSpatialNodeEvaluator()

# example = FEATURE_EXAMPLES[-1]

# example: ExampleEval[AnySpatialNodeType] = ExampleEval(
#     user_text="along the Oman-Yemen border",
#     description="Simple border between two areas that may miss continent",
#     expected_node=BorderBetween(
#         child_node_1=NamedPlace(name="Oman", type=GeoPlaceType.country, in_continent="Asia"),
#         child_node_2=NamedPlace(name="Yemen", type=GeoPlaceType.country, in_continent="Asia"),
#     ),
# )


# eval_result = evaluator.evaluate(llm, example)
# print(eval_result.to_markdown())

# full_eval = evaluator.evaluate_examples(llm, ALL_EXAMPLES)
# print(full_eval.to_markdown())
