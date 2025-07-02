"""Contains functions to calculate the tree edit distance between two spatial nodes."""

from abc import ABC
from datetime import datetime
from enum import Enum
from functools import singledispatch
from typing import Any, cast

from pydantic import BaseModel, ConfigDict, SkipValidation, field_serializer
from shapely.geometry.base import BaseGeometry
from zss import simple_distance  # type: ignore[reportUnknownVariableType]

type _SimpleValue = str | int | float | bool | datetime | BaseGeometry | Enum | None


def _is_simple_value(v: Any) -> bool:  # noqa: ANN401
    return isinstance(v, (str, int, float, bool, datetime, BaseGeometry, Enum)) or v is None


type _ComplexNodeValue = BaseModel | list[BaseModel]

type _Value = _SimpleValue | _ComplexNodeValue


class _Attribute(BaseModel, ABC):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    name: str


class _SimpleAttribute(_Attribute):
    model_config = ConfigDict(
        strict=True, extra="forbid", frozen=True, arbitrary_types_allowed=True
    )
    value: SkipValidation[_SimpleValue]

    @field_serializer("value")
    def _shapely_geometry_to_json(
        self, v: _SimpleValue
    ) -> dict[str, Any] | str | int | float | bool | None:
        if isinstance(v, BaseGeometry):
            return v.__geo_interface__
        if isinstance(v, Enum):
            return v.value
        if isinstance(v, datetime):
            return v.isoformat()
        return v


class _ComplexNodeAttribute(_Attribute):
    value: SkipValidation[_ComplexNodeValue]


def _value_to_attribute(field: str, value: Any) -> _Attribute:  # noqa: ANN401
    if isinstance(value, BaseModel):
        return _ComplexNodeAttribute(name=field, value=value)
    if isinstance(value, list):
        items: list[Any] = cast("list[Any]", value)
        if len(items) > 0 and isinstance(items[0], BaseModel):
            return _ComplexNodeAttribute(name=field, value=items)
        if len(items) == 0:
            return _ComplexNodeAttribute(name=field, value=[])
        raise ValueError(f"Unable to handle list value of: {value}")

    if _is_simple_value(value):
        return _SimpleAttribute(name=field, value=value)
    raise ValueError(f"Unable to handle value of type {type(value)}: {value}")


_GetChildrenResponse = list[_Attribute] | list[BaseModel]


@singledispatch
def _get_children(node: _Value) -> _GetChildrenResponse:
    """Returns the "children" of the node."""
    raise NotImplementedError(f"node of type {node.__class__} is not implemented for _get_children")


@_get_children.register
def _(node: BaseModel) -> _GetChildrenResponse:
    return [_value_to_attribute(field, value) for field, value in node]


@_get_children.register
def _(_node: _SimpleAttribute) -> _GetChildrenResponse:
    return []


@_get_children.register
def _(node: _ComplexNodeAttribute) -> _GetChildrenResponse:
    if isinstance(node.value, list):
        return node.value
    return _get_children(node.value)


def _get_label(node: _Attribute | BaseModel) -> str:
    """Converts a node to string that can be used for comparision to see if it's equal.

    Children do not need to be part of the label.
    """
    if isinstance(node, _SimpleAttribute):
        return node.model_dump_json()

    if isinstance(node, _ComplexNodeAttribute):
        return node.name

    return node.__class__.__name__


def tree_to_markdown(node: _Attribute | BaseModel, indent: str = "") -> str:
    """Prints a node as a tree as this module would represent it.

    Helps in debugging tree edit distance.
    """
    label = _get_label(node)
    return "\n".join(
        [
            f"{indent}* {label}",
            *[tree_to_markdown(child, indent=indent + "  ") for child in _get_children(node)],
        ]
    )


def _label_distance(l1: str, l2: str) -> float:
    """Returns the edit distance between two node values."""
    # FUTURE it may make more sense to use a real edit distance here.
    # zss uses https://pypi.org/project/editdistance/
    if l1 == l2:
        return 0
    return 1


def get_tree_edit_distance(node1: BaseModel, node2: BaseModel) -> float:
    """Returns the edit distance between two nodes.

    0 indicates that the nodes are identical. The distance gets larger for the more changes that are
    required to make the nodes match.
    """
    resp = simple_distance(node1, node2, _get_children, _get_label, _label_distance)  # type: ignore[reportUnknownArgumentType]
    distance: float = float(resp)  # type: ignore[reportUnknownArgumentType]

    # Safety checks that our distance algorithm is working.
    if node1 == node2 and distance != 0.0:
        raise Exception("Expected two nodes that are equivalent to have an edit distance of 0")

    if distance == 0.0 and node1 != node2:
        raise Exception("Expected two nodes to be equal with an edit distance of 0")

    return distance
