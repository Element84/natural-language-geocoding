import difflib
from typing import Any

from pydantic import BaseModel


def _dump_for_diff(m: BaseModel) -> list[str]:
    return m.model_dump_json(indent=2).splitlines(keepends=True)


def pytest_assertrepr_compare(op: str, left: Any, right: Any) -> list[str] | None:  # noqa: ANN401
    """Shows a diff of the JSON representation of pydantic models when an equality check fails."""
    if op == "==" and isinstance(left, BaseModel) and isinstance(right, BaseModel):
        return list(difflib.unified_diff(_dump_for_diff(right), _dump_for_diff(left)))
    return None
