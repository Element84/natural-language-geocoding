from e84_geoai_common.llm.core import LLMInferenceConfig, LLMMessage, TextContent
from e84_geoai_common.llm.models import CLAUDE_BEDROCK_MODEL_IDS, BedrockClaudeLLM

import natural_language_geocoding.prompt
from natural_language_geocoding.eval.evaluator import evaluate_examples

haiku_llm = BedrockClaudeLLM()
sonnet_llm = BedrockClaudeLLM(model_id=CLAUDE_BEDROCK_MODEL_IDS["Claude 3.5 Sonnet v2"])

full_eval = evaluate_examples(haiku_llm)

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

config = LLMInferenceConfig(
    system_prompt=system_prompt,
)
resp = sonnet_llm.prompt(
    messages=[
        LLMMessage(
            content=[
                TextContent(
                    text=f"<prompt>{natural_language_geocoding.prompt.SYSTEM_PROMPT}</prompt>"
                ),
                TextContent(
                    text=f"<evaluation_result>{full_eval.to_markdown()}</evaluation_result>"
                ),
            ]
        )
    ],
    inference_cfg=config,
)


print(resp.to_text_only())  # noqa: T201
