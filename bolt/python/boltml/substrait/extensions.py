# Copyright (c) ByteDance Ltd. and/or its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any, Union

from substrait.proto import extensions
from pybolt import BoltType, UnknownType

from ..expression.base import Expression
from ..expression.function import FunctionExpression

from .signature import substraitSignature

Extension = extensions.SimpleExtensionDeclaration


def extensionIndex(e: Extension):
    kind = e.WhichOneof("mapping_type")
    if kind == "extension_type":
        return e.extension_type.type_anchor
    if kind == "extension_function":
        return e.extension_function.function_anchor
    if kind == "extension_type_variation":
        return e.extension_type_variation.type_variation_anchor
    raise RuntimeError(f"Unrecognized substrait extension: {e}")


def findExtensionIndices(msg) -> set[int]:
    """
    Recursively search a substrait protobuf message for extension indices.
    """

    # The list of substrait message fields storing an extension reference.
    # The list needs to be updated if we use additional fields or custom
    # protobuf message fields.
    referenceFields = ("function_reference", "type_reference")
    try:
        fields = msg.ListFields()
    except Exception:
        return set()

    anchors: set[int] = set()
    for field, value in fields:
        if field.name in referenceFields:
            anchors.add(int(value))
        elif field.is_repeated:
            for item in value:
                if hasattr(item, "ListFields"):
                    anchors.update(findExtensionIndices(item))
        elif field.type == field.TYPE_MESSAGE and hasattr(value, "ListFields"):
            anchors.update(findExtensionIndices(value))
    return anchors


class ExtensionRegister:
    def __init__(self):
        self.__nextExtensionIdx = 1
        self.__extensions: dict[str, Extension] = {}

    @staticmethod
    def _makeKey(arg: Union[Expression, BoltType]) -> str:
        if isinstance(arg, Expression):
            return arg.expr()
        if isinstance(arg, BoltType):
            return str(arg)
        raise TypeError(f"Invalid ExtensionRegister key type: {type(arg)}")

    def _makeExtension(self, arg: Union[FunctionExpression, BoltType]):
        if isinstance(arg, FunctionExpression):
            signature = substraitSignature(arg)
            return Extension(
                extension_function=Extension.ExtensionFunction(
                    function_anchor=self.__nextExtensionIdx,
                    name=signature,
                )
            )
        if isinstance(arg, UnknownType):
            return Extension(
                extension_type=Extension.ExtensionType(
                    type_anchor=self.__nextExtensionIdx, name="UNKNOWN"
                )
            )
        raise TypeError(
            f"Unsupported substrait extension conversion from type: {type(arg)}"
        )

    def __getitem__(self, arg: Any):
        key = ExtensionRegister._makeKey(arg)
        try:
            return self.__extensions[key]
        except KeyError:
            extension = self._makeExtension(arg)
            self.__extensions[key] = extension
            self.__nextExtensionIdx += 1
            return extension

    def getAll(self) -> list[Extension]:
        """
        Returns all registered substrait extensions sorted by their extension index.
        """
        extensions = [e for _, e in self.__extensions.items()]
        return sorted(extensions, key=lambda e: extensionIndex(e))

    def find(self, msg) -> list[Extension]:
        """
        Find all extensions referenced in a substrait message.
        """
        anchors = findExtensionIndices(msg)
        return [e for e in self.__extensions.values() if extensionIndex(e) in anchors]


extensionRegister = ExtensionRegister()
