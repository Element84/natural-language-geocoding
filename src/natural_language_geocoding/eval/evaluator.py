"""Defines examples and methods for evaluating natural language geocoding."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import deepdiff.serialization
from deepdiff import DeepDiff
from e84_geoai_common.llm.core import LLM
from e84_geoai_common.llm.models import CLAUDE_BEDROCK_MODEL_IDS, BedrockClaudeLLM
from pydantic import BaseModel, ConfigDict, Field

from natural_language_geocoding import parse_spatial_node_from_text
from natural_language_geocoding.eval.tree_distance import get_spatial_node_tree_distance
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
    SpatialNode,
    Union,
)

logger = logging.getLogger(__name__)
_TEMP_DIR = Path("temp")


def _load_template(name: str) -> str:
    path = Path(__file__).parent / "templates" / name

    with path.open() as f:
        return f.read()


_EVAL_SUCCESS_TEMPLATE = _load_template("single_eval_success_result.md")
_EVAL_DIFF_TEMPLATE = _load_template("single_eval_diff_result.md")
_FULL_EVAL_TEMPLATE = _load_template("full_eval_result.md")


class ExampleNLG(BaseModel):
    """A single example of natural language geocoding.."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    user_text: str = Field(description="The input user text")
    description: str | None = Field(description="A description of the example.", default=None)
    expected_node: AnySpatialNodeType = Field(
        description="The expected spatial node that the LLM will parse"
    )


NAMED_PLACE_EXAMPLES: list[ExampleNLG] = [
    ExampleNLG(
        user_text="Paris",
        description="Simple city lookup",
        expected_node=NamedPlace(
            name="Paris",
            type=GeoPlaceType.locality,
            in_continent="Europe",
            in_country="France",
        ),
    ),
    ExampleNLG(
        user_text="France",
        description="Simple country lookup",
        expected_node=NamedPlace(
            name="France",
            type=GeoPlaceType.country,
            in_continent="Europe",
        ),
    ),
    ExampleNLG(
        user_text="Paris, France",
        description="City scoped to a country",
        expected_node=NamedPlace(
            name="Paris",
            type=GeoPlaceType.locality,
            in_continent="Europe",
            in_country="France",
        ),
    ),
    ExampleNLG(
        user_text="United States",
        description="USA lookup",
        expected_node=NamedPlace(
            name="United States",
            type=GeoPlaceType.country,
            in_continent="North America",
        ),
    ),
    ExampleNLG(
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
    ExampleNLG(
        user_text="Mississippi",
        description="Ambiguous state or river defaults to state",
        expected_node=NamedPlace(
            name="Mississippi",
            type=GeoPlaceType.region,
            in_continent="North America",
            in_country="United States",
        ),
    ),
    ExampleNLG(
        user_text="Mississippi River",
        description="River",
        expected_node=NamedPlace(
            name="Mississippi River",
            type=GeoPlaceType.river,
            in_continent="North America",
            in_country="United States",
        ),
    ),
    ExampleNLG(
        user_text="Maui",
        description="Island",
        expected_node=NamedPlace(
            name="Maui",
            type=GeoPlaceType.island,
            in_continent="Oceania",
            in_country="United States",
            in_region="Hawaii",
        ),
    ),
]

FEATURE_EXAMPLES: list[ExampleNLG] = [
    ExampleNLG(
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
    ExampleNLG(
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
    ExampleNLG(
        user_text="Border between France and Spain",
        description="Simple border between two areas",
        expected_node=BorderBetween(
            child_node_1=NamedPlace(
                name="France", type=GeoPlaceType.country, in_continent="Europe"
            ),
            child_node_2=NamedPlace(name="Spain", type=GeoPlaceType.country, in_continent="Europe"),
        ),
    ),
    ExampleNLG(
        user_text="Border of France",
        description="Simple border extraction",
        expected_node=BorderOf(
            child_node=NamedPlace(name="France", type=GeoPlaceType.country, in_continent="Europe")
        ),
    ),
    ExampleNLG(
        user_text="Within 10 miles of France",
        description="Buffer of an area",
        expected_node=Buffer(
            child_node=NamedPlace(name="France", type=GeoPlaceType.country, in_continent="Europe"),
            distance=10,
            distance_unit="miles",
        ),
    ),
    ExampleNLG(
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
    ExampleNLG(
        user_text="France and Spain",
        description="Simple union",
        expected_node=Union(
            child_nodes=[
                NamedPlace(name="France", type=GeoPlaceType.country, in_continent="Europe"),
                NamedPlace(name="Spain", type=GeoPlaceType.country, in_continent="Europe"),
            ]
        ),
    ),
    ExampleNLG(
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
    ExampleNLG(
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
                name="Washington DC",
                type=GeoPlaceType.locality,
                in_continent="North America",
                in_country="United States",
            ),
        ),
    ),
]

ALL_EXAMPLES = [*NAMED_PLACE_EXAMPLES, *FEATURE_EXAMPLES]


class SingleEvaluation(BaseModel):
    """The result of evaluating a single example."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    example: ExampleNLG = Field(description="The original example to evaluate against")
    actual: SpatialNode = Field(description="The actual spatial node produced by the model")
    tree_distance: float = Field(
        description=(
            "The tree edit distance between expected and actual nodes. 0 means they are identical."
        )
    )
    diff_explanations: list[str] = Field(
        description="Human-readable explanations of differences between nodes"
    )

    @property
    def is_success(self) -> bool:
        return self.tree_distance == 0.0

    def to_markdown(self) -> str:
        description = f"\n{self.example.description}\n" if self.example.description else ""
        if self.is_success:
            return _EVAL_SUCCESS_TEMPLATE.format(eval=self, description=description)

        return _EVAL_DIFF_TEMPLATE.format(
            eval=self,
            description=description,
            diff_explanations="\n".join(
                [f"* {explanation}" for explanation in self.diff_explanations]
            ),
            expected_json=self.example.expected_node.model_dump_json(indent=2),
            actual_json=self.actual.model_dump_json(indent=2),
        )


class Evaluations(BaseModel):
    """The result of evaluating a set of examples."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    evaluations: list[SingleEvaluation]

    @property
    def distance(self) -> float:
        """The total tree edit distance for all evaluations."""
        return sum([e.tree_distance for e in self.evaluations])

    def to_markdown(self) -> str:
        total_evals = len(self.evaluations)
        num_successful = len([e for e in self.evaluations if e.is_success])
        num_failed = len([e for e in self.evaluations if not e.is_success])
        pct_success = int(num_successful / total_evals) * 100

        child_eval_markdown = "\n\n".join([e.to_markdown() for e in self.evaluations])

        return _FULL_EVAL_TEMPLATE.format(
            total_evals=total_evals,
            num_successful=num_successful,
            num_failed=num_failed,
            pct_success=pct_success,
            total_distance=self.distance,
            single_eval_markdown=child_eval_markdown,
        )


def evaluate(llm: LLM, example: ExampleNLG) -> SingleEvaluation:
    """Evaluates a single example."""
    logger.info("Evaluating example [%s]", example.user_text)
    actual = parse_spatial_node_from_text(llm, example.user_text)
    distance = get_spatial_node_tree_distance(actual.root, example.expected_node)
    logger.info("Example [%s] tree distance is %s", example.user_text, distance)

    diff_explanations: list[str] = []
    if distance > 0:
        expected_dict = example.expected_node.model_dump()
        actual_dict = actual.model_dump()
        diff = DeepDiff(expected_dict, actual_dict, verbose_level=2, view="tree")
        diff_tree: dict[str, Any] = cast("dict[str, Any]", diff.tree)

        diff_explanations = [
            deepdiff.serialization.pretty_print_diff(item_key)  # type: ignore[reportUnknownArgumentType]
            for key in sorted(diff_tree.keys())
            for item_key in diff_tree[key]
        ]
        logger.info("Actual result %s", actual.model_dump_json(indent=2))
    return SingleEvaluation(
        example=example, actual=actual, tree_distance=distance, diff_explanations=diff_explanations
    )


def evaluate_examples(llm: LLM) -> Evaluations:
    """Evaluates all of the examples defined in this module."""
    return Evaluations(evaluations=[evaluate(llm, example) for example in ALL_EXAMPLES])


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    llm = BedrockClaudeLLM(model_id=CLAUDE_BEDROCK_MODEL_IDS["Claude 3.7 Sonnet"])
    full_eval = evaluate_examples(llm)
    print(full_eval.to_markdown())

    date_str = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")

    with (_TEMP_DIR / f"full_eval_{date_str}.md").open("w") as f:
        f.write(full_eval.to_markdown())


#################
# code for manual testing
# ruff: noqa: ERA001,T201

# llm = BedrockClaudeLLM(model_id=CLAUDE_BEDROCK_MODEL_IDS["Claude 3.7 Sonnet"])
# example = ExampleNLG(
#     user_text="Maui",
#     description="Island",
#     expected_node=NamedPlace(
#         name="Maui",
#         type=GeoPlaceType.island,
#         in_continent="Oceania",
#         in_country="United States",
#         in_region="Hawaii",
#     ),
# )

# eval_result = evaluate(llm, example)
# print(eval_result.to_markdown())

# full_eval = evaluate_examples(llm)
# print(full_eval.to_markdown())
