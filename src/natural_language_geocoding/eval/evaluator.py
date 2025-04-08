"""TODO docs."""

import logging
import re
from pathlib import Path
from typing import Any, cast

import deepdiff.serialization
from deepdiff import DeepDiff
from e84_geoai_common.llm.core import LLM
from e84_geoai_common.llm.models import CLAUDE_BEDROCK_MODEL_IDS, BedrockClaudeLLM
from pydantic import BaseModel, ConfigDict

from natural_language_geocoding import parse_spatial_node_from_text
from natural_language_geocoding.eval.tree_distance import get_spatial_node_tree_distance
from natural_language_geocoding.geocode_index.geoplace import GeoPlaceType
from natural_language_geocoding.models import (
    AnySpatialNodeType,
    BorderBetween,
    CoastOf,
    DirectionalConstraint,
    NamedPlace,
    SpatialNode,
)

# Improvements
# - Make an example class with a description
# - Create an example report class
# - Include a diff of the trees in the result.
# - Need a way to visualize the trees easily and for human analysis.

logger = logging.getLogger(__name__)


def _load_template(name: str) -> str:
    path = Path(__file__).parent / "templates" / name

    with path.open() as f:
        return f.read()


_EVAL_SUCCESS_TEMPLATE = _load_template("single_eval_success_result.md")
_EVAL_DIFF_TEMPLATE = _load_template("single_eval_diff_result.md")
_FULL_EVAL_TEMPLATE = _load_template("full_eval_result.md")


class ExampleNLG(BaseModel):
    """TODO docs."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    user_text: str
    description: str | None = None
    expected_node: AnySpatialNodeType


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
        description="Ambiguous state or river",
        expected_node=NamedPlace(
            name="Mississippi",
            in_continent="North America",
            in_country="United States",
        ),
    ),
    ExampleNLG(
        user_text="Mississippi River",
        description="River",
        expected_node=NamedPlace(
            name="Mississippi",
            type=GeoPlaceType.river,
            in_continent="North America",
            in_country="United States",
        ),
    ),
    # TODO add islands
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
        description="Simple border extraction",
        expected_node=BorderBetween(
            child_node_1=NamedPlace(
                name="France",
                type=GeoPlaceType.country,
                in_continent="Europe",
            ),
            child_node_2=NamedPlace(
                name="Spain",
                type=GeoPlaceType.country,
                in_continent="Europe",
            ),
        ),
    ),
]

ALL_EXAMPLES = [*NAMED_PLACE_EXAMPLES, *FEATURE_EXAMPLES]


class SingleEvaluation(BaseModel):
    """TODO docs."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    example: ExampleNLG
    actual: SpatialNode
    tree_distance: float
    diff_explanations: list[str]

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


class FullEvaluation(BaseModel):
    """TODO docs."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    evaluations: list[SingleEvaluation]

    @property
    def distance(self) -> float:
        return sum([e.tree_distance for e in self.evaluations])

    def to_markdown(self) -> str:
        total_evals = len(self.evaluations)
        num_successful = len([e for e in self.evaluations if e.is_success])
        num_failed = len([e for e in self.evaluations if not e.is_success])
        pct_success = int(num_successful / total_evals)

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
    """TODO docs."""
    logger.info("Evaluating example [%s]", example.user_text)
    actual = parse_spatial_node_from_text(llm, example.user_text)
    distance = get_spatial_node_tree_distance(actual.root, example.expected_node)

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
    return SingleEvaluation(
        example=example, actual=actual, tree_distance=distance, diff_explanations=diff_explanations
    )


def evaluate_examples(llm: LLM) -> FullEvaluation:
    """TODO docs."""
    return FullEvaluation(evaluations=[evaluate(llm, example) for example in ALL_EXAMPLES])


#################
# code for manual testing
# ruff: noqa: ERA001,T201

llm = BedrockClaudeLLM(model_id=CLAUDE_BEDROCK_MODEL_IDS["Claude 3.7 Sonnet"])


example = ExampleNLG(
    user_text="Panama Canal",
    expected_node=NamedPlace(name="Panama Canal", type=GeoPlaceType.river),
)


eval_result = evaluate(llm, example)

print(eval_result.to_markdown())


full_eval = evaluate_examples(llm)
print(full_eval.to_markdown())


_TEMP_DIR = Path("temp")


# Find the highest existing version number
def _get_next_version() -> int:
    pattern = re.compile(r"temp/full_eval_v(\d+)\.md$")
    max_version = -1

    for filename in _TEMP_DIR.iterdir():
        match = pattern.match(str(filename))
        if match:
            version = int(match.group(1))
            max_version = max(max_version, version)

    # Return the next version number
    return max_version + 1


test_version = _get_next_version()

with (_TEMP_DIR / f"full_eval_v{test_version}.md").open("w") as f:
    f.write(full_eval.to_markdown())
