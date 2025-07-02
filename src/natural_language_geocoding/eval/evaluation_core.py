"""Defines examples and methods for evaluating natural language geocoding.

Eventually we want to move this to e84-geoai-common once we establish what a general evaluation
framework looks like consistently.
"""

import concurrent.futures
import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import deepdiff.serialization
from deepdiff import DeepDiff
from e84_geoai_common.llm.core import LLM
from pydantic import BaseModel, ConfigDict, Field

from natural_language_geocoding.eval.tree_distance import get_tree_edit_distance

logger = logging.getLogger(__name__)
_TEMP_DIR = Path("temp")


def _load_template(name: str) -> str:
    path = Path(__file__).parent / "templates" / name

    with path.open() as f:
        return f.read()


_EVAL_SUCCESS_TEMPLATE = _load_template("single_eval_success_result.md")
_EVAL_DIFF_TEMPLATE = _load_template("single_eval_diff_result.md")
_FULL_EVAL_TEMPLATE = _load_template("full_eval_result.md")


class ExampleEval[T: BaseModel](BaseModel):
    """A single example evaluating an LLM.

    It defines the input user text and the expected parsed object from the LLM response.
    """

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    user_text: str = Field(description="The input user text")
    description: str | None = Field(description="A description of the example.", default=None)
    expected_node: T = Field(description="The expected parsed node")


class SingleEvaluation[T: BaseModel](BaseModel):
    """The result of evaluating a single example."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    example: ExampleEval[T] = Field(description="The original example to evaluate against")
    actual: T = Field(description="The actual node produced by the model")
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


class Evaluations[T: BaseModel](BaseModel):
    """The result of evaluating a set of examples."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    evaluations: list[SingleEvaluation[T]]

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

    def save(self) -> None:
        date_str = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")

        with (_TEMP_DIR / f"full_eval_{date_str}.md").open("w") as f:
            f.write(self.to_markdown())


class Evaluator[T: BaseModel](ABC):
    """Provides a base class for evaluating the ability to extract data from natural language."""

    @abstractmethod
    def parse(self, llm: LLM, user_text: str) -> T:
        """Subclases implement this to parse the text given an LLM and return the parsed object."""
        ...

    def get_edit_distance(self, node1: T, node2: T) -> float:
        """Returns the tree edit distance between the two nodes."""
        return get_tree_edit_distance(node1, node2)

    def evaluate(self, llm: LLM, example: ExampleEval[T]) -> SingleEvaluation[T]:
        """Evaluates a single example."""
        logger.info("Evaluating example [%s]", example.user_text)
        actual = self.parse(llm, example.user_text)
        distance = self.get_edit_distance(actual, example.expected_node)
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
            example=example,
            actual=actual,
            tree_distance=distance,
            diff_explanations=diff_explanations,
        )

    def evaluate_examples(
        self, llm: LLM, examples: Sequence[ExampleEval[T]], *, max_concurrent: int = 5
    ) -> Evaluations[T]:
        """Evaluates all of the examples defined in this module in parallel."""
        indexed_evaluations: list[tuple[int, SingleEvaluation[T]]] = []

        def eval_with_index(
            llm: LLM, example: ExampleEval[T], index: int
        ) -> tuple[int, SingleEvaluation[T]]:
            return (index, self.evaluate(llm, example))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            # Submit all evaluation tasks to the executor
            future_to_example = {
                executor.submit(eval_with_index, llm, example, index): example
                for index, example in enumerate(examples)
            }

            # Collect results as they complete
            for future in concurrent.futures.as_completed(future_to_example):
                example = future_to_example[future]
                try:
                    index, evaluation = future.result()
                    indexed_evaluations.append((index, evaluation))
                except Exception:
                    logger.exception("Evaluation failed for example '%s'", example.user_text)
                    raise

        return Evaluations(
            # Return the evaluations in order
            evaluations=[
                evaluation for _, evaluation in sorted(indexed_evaluations, key=lambda i_e: i_e[0])
            ]
        )
