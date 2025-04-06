"""TODO docs."""

from abc import ABC
from functools import singledispatch

from pydantic import BaseModel, ConfigDict, SkipValidation
from zss import simple_distance  # type: ignore[reportUnknownVariableType]

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
    SpatialNodeType,
    Union,
)

_SimpleValue = str | int | float | bool | None
_SpatialNodeValue = SpatialNodeType | list[SpatialNodeType]
_Value = _SimpleValue | _SpatialNodeValue


class _Attribute(BaseModel, ABC):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    name: str


class _SimpleAttribute(_Attribute):
    value: SkipValidation[_SimpleValue]


class _SpatialNodeAttribute(_Attribute):
    value: SkipValidation[_SpatialNodeValue]


def _value_to_attribute(field: str, value: _Value) -> _Attribute:
    if isinstance(value, SpatialNodeType):
        return _SpatialNodeAttribute(name=field, value=value)
    if (
        isinstance(value, list) and len(value) > 0 and isinstance(value[0], SpatialNodeType)  # type: ignore[reportUnnecessaryIsInstance]
    ):
        return _SpatialNodeAttribute(name=field, value=value)

    return _SimpleAttribute(name=field, value=value)  # type: ignore[reportArgumentType]


_GetChildrenResponse = list[_Attribute] | list[SpatialNodeType]


@singledispatch
def _get_children(node: _Value) -> _GetChildrenResponse:
    raise NotImplementedError(f"node of type {node.__class__} is not implemented for _get_children")


@_get_children.register
def _(node: SpatialNodeType) -> _GetChildrenResponse:
    return [_value_to_attribute(field, value) for field, value in node if field != "node_type"]


# Register the subclasses of SpatialNodeType explicitly
@_get_children.register(NamedPlace)
@_get_children.register(Buffer)
@_get_children.register(BorderBetween)
@_get_children.register(BorderOf)
@_get_children.register(CoastOf)
@_get_children.register(Intersection)
@_get_children.register(Union)
@_get_children.register(Difference)
@_get_children.register(Between)
@_get_children.register(DirectionalConstraint)
def _(node: SpatialNodeType) -> _GetChildrenResponse:
    # Call the implementation for SpatialNodeType
    return _get_children.registry[SpatialNodeType](node)


@_get_children.register
def _(_node: _SimpleAttribute) -> _GetChildrenResponse:
    return []


@_get_children.register
def _(node: _SpatialNodeAttribute) -> _GetChildrenResponse:
    if isinstance(node.value, list):
        return node.value
    return _get_children(node.value)


def _get_label(node: _Attribute | SpatialNodeType) -> str:
    if isinstance(node, _SimpleAttribute):
        return node.model_dump_json()

    if isinstance(node, _SpatialNodeAttribute):
        return node.name

    return node.__class__.__name__


def _label_distance(l1: str, l2: str) -> float:
    # FUTURE it may make more sense to use a real edit distance here.
    # zss uses https://pypi.org/project/editdistance/
    if l1 == l2:
        return 0
    return 1


# TODO unit test this method
def get_spatial_node_tree_distance(node1: AnySpatialNodeType, node2: AnySpatialNodeType) -> float:
    """TODO docs."""
    # TODO add safety check in here. If the nodes are not equal then the distance must not be 0.
    resp = simple_distance(node1, node2, _get_children, _get_label, _label_distance)  # type: ignore[reportUnknownArgumentType]
    return float(resp)  # type: ignore[reportUnknownArgumentType]


#################
# code for manual testing
# ruff: noqa: ERA001

# node = Intersection.from_nodes(
#     NamedPlace(name="alpha"),
#     DirectionalConstraint(child_node=NamedPlace(name="bravo"), direction="north"),
# )
# node2 = Intersection.from_nodes(
#     NamedPlace(name="bravo"),
#     DirectionalConstraint(child_node=NamedPlace(name="bravo"), direction="north"),
# )
# node3 = Intersection.from_nodes(
#     NamedPlace(name="bravo"),
#     DirectionalConstraint(child_node=NamedPlace(name="charlie"), direction="north"),
# )
# node4 = Intersection.from_nodes(
#     NamedPlace(name="bravo"),
#     NamedPlace(name="bravo"),
# )
# node5 = Intersection.from_nodes(
#     NamedPlace(name="bravo"),
#     NamedPlace(name="charlie"),
# )

# float(simple_distance(node, node, _get_children, _get_label, _label_distance))

# [
#     ("node", simple_distance(node, node, _get_children, _get_label, _label_distance)),
#     ("node2", simple_distance(node, node2, _get_children, _get_label, _label_distance)),
#     ("node3", simple_distance(node, node3, _get_children, _get_label, _label_distance)),
#     ("node4", simple_distance(node, node4, _get_children, _get_label, _label_distance)),
#     ("node5", simple_distance(node, node5, _get_children, _get_label, _label_distance)),
# ]
