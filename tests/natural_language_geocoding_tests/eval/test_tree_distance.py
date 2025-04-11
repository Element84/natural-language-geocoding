from abc import ABC
from enum import Enum
from textwrap import dedent
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_serializer
from shapely import Point
from shapely.geometry.base import BaseGeometry

from natural_language_geocoding.eval.tree_distance import (
    get_tree_edit_distance,
    tree_to_markdown,
)


class _FileSystemEntity(BaseModel, ABC):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    name: str
    permissions: int


class _Directory(_FileSystemEntity):
    node_type: Literal["dir"] = "dir"
    contents: "list[_AnyFileSystemEntity]"


class _File(_FileSystemEntity):
    node_type: Literal["file"] = "file"
    size: int


class _SymLink(_FileSystemEntity):
    node_type: Literal["link"] = "link"
    target: str


class _GeoFile(_File):
    model_config = ConfigDict(
        strict=True, extra="forbid", frozen=True, arbitrary_types_allowed=True
    )
    geom: BaseGeometry

    @field_serializer("geom")
    def _shapely_geometry_to_json(self, g: BaseGeometry) -> dict[str, Any]:
        return g.__geo_interface__


class _Color(Enum):
    red = "red"
    green = "green"
    blue = "blue"


class _EnumFile(_File):
    color: _Color


_AnyFileSystemEntity = _Directory | _File | _SymLink


filesystem = _Directory(
    name="root",
    permissions=123,
    contents=[
        _Directory(
            name="stuff",
            permissions=123,
            contents=[
                _File(name="notes.txt", permissions=123, size=145),
                _GeoFile(name="todo_list.md", permissions=123, size=145, geom=Point(1, 2)),
            ],
        ),
        _EnumFile(name="todo_list.md", permissions=123, size=145, color=_Color.red),
    ],
)

expected_fs_tree = dedent("""
  * _Directory
    * {"name":"name","value":"root"}
    * {"name":"permissions","value":123}
    * {"name":"node_type","value":"dir"}
    * contents
      * _Directory
        * {"name":"name","value":"stuff"}
        * {"name":"permissions","value":123}
        * {"name":"node_type","value":"dir"}
        * contents
          * _File
            * {"name":"name","value":"notes.txt"}
            * {"name":"permissions","value":123}
            * {"name":"node_type","value":"file"}
            * {"name":"size","value":145}
          * _GeoFile
            * {"name":"name","value":"todo_list.md"}
            * {"name":"permissions","value":123}
            * {"name":"node_type","value":"file"}
            * {"name":"size","value":145}
            * {"name":"geom","value":{"type":"Point","coordinates":[1.0,2.0]}}
      * _EnumFile
        * {"name":"name","value":"todo_list.md"}
        * {"name":"permissions","value":123}
        * {"name":"node_type","value":"file"}
        * {"name":"size","value":145}
        * {"name":"color","value":"red"}
""").strip()


def test_get_labels_and_children() -> None:
    """Indirectly tests the _get_label and _get_children functions through tree_to_markdown.

    This allows a visual comparison to ensure that every node of the tree is visited and every
    attribute is shown in the printed data.
    """
    assert tree_to_markdown(filesystem) == expected_fs_tree


def test_get_tree_edit_distance_simple() -> None:
    file1 = _File(name="notes.txt", permissions=123, size=145)
    # Identical
    assert get_tree_edit_distance(file1, _File(name="notes.txt", permissions=123, size=145)) == 0
    # Change one attribute
    assert get_tree_edit_distance(file1, _File(name="other.txt", permissions=123, size=145)) == 1.0
    # Change two attributes
    assert get_tree_edit_distance(file1, _File(name="other.txt", permissions=124, size=145)) == 2.0
    # Change three attributes
    assert get_tree_edit_distance(file1, _File(name="other.txt", permissions=124, size=200)) == 3.0


def test_get_tree_edit_distance_simple_geom() -> None:
    file1 = _GeoFile(name="notes.txt", permissions=123, size=145, geom=Point(1, 2))
    # Identical
    assert (
        get_tree_edit_distance(
            file1, _GeoFile(name="notes.txt", permissions=123, size=145, geom=Point(1, 2))
        )
        == 0
    )
    # Change geom
    assert (
        get_tree_edit_distance(
            file1, _GeoFile(name="notes.txt", permissions=123, size=145, geom=Point(3, 2))
        )
        == 1.0
    )


def test_get_tree_edit_distance_simple_enum() -> None:
    file1 = _EnumFile(name="todo_list.md", permissions=123, size=145, color=_Color.red)
    # Identical
    assert (
        get_tree_edit_distance(
            file1, _EnumFile(name="todo_list.md", permissions=123, size=145, color=_Color.red)
        )
        == 0
    )
    # Change enum value
    assert (
        get_tree_edit_distance(
            file1, _EnumFile(name="todo_list.md", permissions=123, size=145, color=_Color.green)
        )
        == 1.0
    )


def test_get_tree_edit_distance_nested() -> None:
    fs2 = _Directory(
        name="root",
        permissions=123,
        contents=[
            _Directory(
                name="stuff",
                permissions=123,
                contents=[
                    _File(name="notes.txt", permissions=123, size=145),
                    _GeoFile(name="todo_list.md", permissions=123, size=145, geom=Point(1, 2)),
                ],
            ),
            _EnumFile(name="todo_list.md", permissions=123, size=145, color=_Color.red),
        ],
    )
    # Identical
    assert get_tree_edit_distance(filesystem, fs2) == 0
    assert get_tree_edit_distance(fs2, filesystem) == 0

    # Change one attribute in a child
    fs2 = _Directory(
        name="root",
        permissions=123,
        contents=[
            _Directory(
                name="stuff",
                permissions=123,
                contents=[
                    _File(name="notes.txt", permissions=999, size=145),
                    _GeoFile(name="todo_list.md", permissions=123, size=145, geom=Point(1, 2)),
                ],
            ),
            _EnumFile(name="todo_list.md", permissions=123, size=145, color=_Color.red),
        ],
    )
    assert get_tree_edit_distance(filesystem, fs2) == 1
    assert get_tree_edit_distance(fs2, filesystem) == 1

    # Change two children
    fs2 = _Directory(
        name="root",
        permissions=123,
        contents=[
            _Directory(
                name="stuff",
                permissions=123,
                contents=[
                    _File(name="notes.txt", permissions=999, size=145),
                    _GeoFile(name="modified.md", permissions=123, size=145, geom=Point(1, 2)),
                ],
            ),
            _EnumFile(name="todo_list.md", permissions=123, size=145, color=_Color.red),
        ],
    )
    assert get_tree_edit_distance(filesystem, fs2) == 2
    assert get_tree_edit_distance(fs2, filesystem) == 2

    # Delete a child
    fs2 = _Directory(
        name="root",
        permissions=123,
        contents=[
            _Directory(
                name="stuff",
                permissions=123,
                contents=[
                    _File(name="notes.txt", permissions=123, size=145),
                ],
            ),
            _EnumFile(name="todo_list.md", permissions=123, size=145, color=_Color.red),
        ],
    )
    # 5 attributes in the child plus the missing child itself
    assert get_tree_edit_distance(filesystem, fs2) == 6
    assert get_tree_edit_distance(fs2, filesystem) == 6
