# Copyright (c) ByteDance Ltd. and/or its affiliates.
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

from abc import ABCMeta, abstractmethod

from pybolt import BoltType

from ..executor import Executor
from .vector import ExecutorsArgument, normalizeExecutors


class RegistrableMeta(type):
    """
    Metaclass of StatefulFunction making sure that the StatefulFunction
    has been registered to an executor runtime through the __init__ method.
    """

    def __new__(cls, name, bases, attrs):
        if "__init__" in attrs:
            init = attrs["__init__"]

            def wrapped(self, *args, **kwargs):
                self.__registered = False
                init(self, *args, **kwargs)
                if not self.__registered:
                    raise RuntimeError(
                        f"Stateful Function '{self.functionName()}' was not registered. Did you call super().__init__(executors)?"
                    )

            attrs["__init__"] = wrapped
        return super().__new__(cls, name, bases, attrs)


class StatefulFunctionMeta(RegistrableMeta, ABCMeta):
    """
    Trick to combine abc abstract class and another metaclass.
    """

    pass


class StatefulFunction(metaclass=StatefulFunctionMeta):
    # Counter to make each instance of the same StatefulFunction child
    # class have a unique name.
    __counter: int = 0

    def __init__(
        self,
        executors: ExecutorsArgument = None,
    ):
        """
        All stateful functions need to call this parent __init__ method
        to ensure that the function is registered to an executor.
        """
        self._register(normalizeExecutors(executors))
        self._RegistrableMeta__registered = True

    @abstractmethod
    def _register(self, executors: list[Executor]):
        """
        Private registration method implemented by each type of Stateful
        functions. This does not need to be implemented by the user.
        """
        ...

    def functionName(self) -> str:
        """
        A string identifying the stateful function instance name.
        All instances of a stateful functions should have a distinct name
        or else they will be denied at registration time.
        The user can override it to give it if name conflicts happen.
        """
        if not hasattr(self, "_BOLTML__functionName"):
            self._BOLTML__functionName = (
                f"{self.__class__.__name__}_{StatefulFunction.__counter}"
            )
            StatefulFunction.__counter += 1
        return self._BOLTML__functionName

    @abstractmethod
    def outputType(self) -> BoltType:
        """
        The output type returned by `__call__()` method.
        This needs to be implemented by the user.
        """
        ...

    @abstractmethod
    def __call__(self, *args, **kwargs): ...


class StatefulDataFrameFunction(StatefulFunction):
    def _register(self, executors: list[Executor]):
        from .dataframe import registerDataframeFunction

        registerDataframeFunction(self, self.outputType())


class StatefulVectorFunction(StatefulFunction):
    def _register(self, executors: list[Executor]):
        from .vector import registerVectorFunction

        registerVectorFunction(
            self, self.functionName(), self.outputType(), executors, False
        )


class StatefulScalarFunction(StatefulFunction):
    def _register(self, executors: list[Executor]):
        from .scalar import registerScalarFunction

        registerScalarFunction(self, self.functionName(), self.outputType(), executors)


class StatefulMapBatchFunction(StatefulFunction):
    def _register(self, executors: list[Executor]):
        from .map_batch import registerMapBatchFunction

        registerMapBatchFunction(
            self, self.functionName(), self.outputType(), executors
        )
