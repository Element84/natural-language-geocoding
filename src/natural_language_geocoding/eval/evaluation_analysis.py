import re
from pathlib import Path

from pydantic_ai import Agent
from pydantic_ai.settings import ModelSettings

import natural_language_geocoding.prompt
from natural_language_geocoding import DEFAULT_MODEL
from natural_language_geocoding.eval.evaluator import ALL_EXAMPLES, ParseSpatialNodeEvaluator

llm = DEFAULT_MODEL

evaluator = ParseSpatialNodeEvaluator()
full_eval = evaluator.evaluate_examples(llm, ALL_EXAMPLES)

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

system_prompt = """
You are ane expert in optimizing LLM prompts to make them successful. You produce prompts that work
effectively by carefully analyzing an existing prompt and evaluation results and then making
suggestions to improve the prompt.

Follow these steps to improve the prompt

1. Read the prompt carefully. It is written in XML tags <prompt></prompt>.
2. Read the evaluation results. It is written in XML tags <evaluation_results></evaluation_results>.
3. Generate a summary of the problems uncovered by the evaluation results.
4. Generate a list of problems and issues with the current prompt.
5. Generate an update to the original prompt that should resolve the issues.
"""

agent = Agent(
    llm,
    system_prompt=system_prompt,
    model_settings=ModelSettings(max_tokens=10000, temperature=0),
)
suggestion = agent.run_sync(
    [
        f"<prompt>{natural_language_geocoding.prompt.SYSTEM_PROMPT}</prompt>",
        f"<evaluation_result>{full_eval.to_markdown()}</evaluation_result>",
    ]
).output


print(suggestion)  # noqa: T201

with (_TEMP_DIR / f"llm_suggestion_v{test_version}.md").open("w") as f:
    f.write(suggestion)
