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

"""Testing utilities for BoltML DataFrame operations.

This module provides decorators and factory classes to simplify writing
parametric tests that run across multiple executor and plan builder
combinations. A runtime is represented by a `Runtime` object, which carries
both the executor and plan builder factory used to create the test
`DataFrame`.

**Design Philosophy:**
The utilities in this module follow a factory pattern combined with decorator
pattern to enable declarative test writing. Test methods can be decorated with
`@WithDataFrame.data()`, `@WithDataFrame.random()`, `@WithDataFrame.dtype()`,
or `@WithDataFrame.factory()` to automatically receive a DataFrame parameter,
eliminating boilerplate setup code.

**Key Features:**
- `WithDataFrame` decorator for injecting DataFrames into test methods
- Support for explicit data, random data, dtype-based random data, and custom
  DataFrame factories
- Automatic test execution across multiple runtimes
- Extensible factory pattern for custom DataFrame creation
- Integration with pybolt's test utilities for random data generation

**Usage Patterns:**
1. **Explicit Data Testing:**
   ```python
   @WithDataFrame.data(c0=[1, 2, 3], c1=["a", "b", "c"])
   def test_my_feature(self, df):
       # df is injected for each runtime
       result = df.select("c0")
       # assertions...
   ```

2. **Random Data Testing:**
   ```python
   @WithDataFrame.random(c0=IntGenerator(), c1=StringGenerator(), length=10)
   def test_randomized_input(self, df):
       # df has 10 rows with random ints and strings
       result = df.filter(df.c0 > 5)
       # assertions...
   ```

3. **Random Data For One Type:**
   ```python
   @WithDataFrame.dtype(RowType(["value"], [IntegerType()]), length=10)
   def test_integer_feature(self, df):
       # df has one random IntegerType column named "value"
       self.assertEqual(df["value"].dtype, IntegerType())
   ```

4. **Custom Runtimes:**
   ```python
   @WithDataFrame.data(
       runtimes=[Runtime(LocalExecutor(), BoltPlanBuilderFactory())],
       c0=[1, 2, 3]
   )
   def test_with_custom_setup(self, df):
       # Tests only with the specified runtime
   ```

5. **Custom DataFrame Factory:**
   ```python
   @WithDataFrame.factory(
       lambda plan_builder, executor: DataFrame(
           {"c0": [1, 2, 3]}, executor=executor, planFactory=plan_builder
       )
   )
   def test_custom_dataframe(self, df):
       # Use this form when data must be computed when each runtime starts.
       # This is typically used to test DataFrame construction from data
       # sources.
   ```

**Testing Coverage:**
By default, tests run with:
- Executors: LocalExecutor
- Plan Builders: BoltPlanBuilderFactory, SubstraitPlanBuilderFactory

This ensures cross-compatibility testing across different execution engines
and query plan representations.
"""

from abc import ABC, abstractmethod
from functools import wraps
from typing import Callable, Optional

from pybolt import RowType
from pybolt.test.utils import ColumnGenerator, RowVectorGenerator

from ..dataframe import DataFrame
from ..plan_builder.base import PlanBuilderFactory
from ..plan_builder.bolt import BoltPlanBuilderFactory
from ..plan_builder.substrait import SubstraitPlanBuilderFactory
from ..executor.base import Executor
from ..executor.local import LocalExecutor


class DataFrameFactory(ABC):
    """Abstract base class for DataFrame creation.

    Defines the interface for creating DataFrames with different plan builders
    and executors. Subclasses must implement the `create` method.

    This abstract factory pattern allows for flexible DataFrame creation
    strategies, supporting both explicit data injection and random data
    generation.
    """

    @abstractmethod
    def create(self, plan_builder: PlanBuilderFactory, executor: Executor) -> DataFrame:
        """Create a DataFrame using the specified plan builder and executor.

        Args:
            plan_builder: The plan builder factory to use for query construction.
                This determines how DataFrame operations are translated into
                executable plans.
            executor: The executor to use for running queries. This determines
                where and how the queries are executed.

        Returns:
            A DataFrame instance configured with the given plan builder and executor.
        """
        ...


class LambdaDataFrameFactory(DataFrameFactory):
    """DataFrame factory that uses a lambda function for creation.

    This factory delegates DataFrame creation to a user-provided callable,
    allowing flexible DataFrame construction for custom scenarios.

    **Use Cases:**
    - Creating DataFrames with complex initialization logic
    - Integrating with external data sources
    - Testing with pre-existing DataFrame instances

    **Example:**
    ```python
    factory = LambdaDataFrameFactory(
        lambda pb, ex: DataFrame({"col": [1, 2]}, executor=ex, planFactory=pb)
    )
    ```
    """

    def __init__(self, factory: Callable[[PlanBuilderFactory, Executor], DataFrame]):
        """Initialize the factory with a creation function.

        Args:
            factory: A callable that takes a PlanBuilderFactory and Executor,
                and returns a DataFrame instance. The lambda receives the
                plan builder and executor in that order.
        """
        self.__factory = factory

    def create(self, plan_builder: PlanBuilderFactory, executor: Executor) -> DataFrame:
        """Create a DataFrame by delegating to the stored lambda function.

        Args:
            plan_builder: The plan builder factory to use for query construction.
            executor: The executor to use for running queries.

        Returns:
            A DataFrame instance created by the lambda function, configured
            with the provided plan builder and executor.
        """
        return self.__factory(plan_builder, executor)


class RandomDataFrameFactory(DataFrameFactory):
    """DataFrame factory that generates random data using RowVectorGenerator.

    This factory creates DataFrames with randomly generated data based on
    the provided column generators, enabling robust testing with varied
    input data.

    **Use Cases:**
    - Testing edge cases and boundary conditions
    - Validating schema correctness across different data types
    - Performance testing with varying data distributions
    - Property-based testing scenarios

    **Example:**
    ```python
    from pybolt.test.utils import IntGenerator, StringGenerator

    factory = RandomDataFrameFactory(
        generator=RowVectorGenerator(c0=IntGenerator(), c1=StringGenerator()),
        length=100
    )
    ```
    """

    def __init__(self, generator: RowVectorGenerator, length: int = 4):
        """Initialize the factory with a row vector generator.

        Args:
            generator: A RowVectorGenerator instance that defines how to
                generate random data for each column. Each column should have
                a corresponding ColumnGenerator.
            length: The number of rows to generate (default: 4). This determines
                the size of the resulting DataFrame.
        """
        self.__generator = generator
        self.__length = length

    def create(self, plan_builder: PlanBuilderFactory, executor: Executor) -> DataFrame:
        """Create a DataFrame with randomly generated data.

        Args:
            plan_builder: The plan builder factory to use for query construction.
            executor: The executor to use for running queries.

        Returns:
            A DataFrame with randomly generated rows, where each column's
            values are generated according to its ColumnGenerator.
        """
        return DataFrame(
            self.__generator.generate_vector(self.__length),
            executor=executor,
            planFactory=plan_builder,
        )


class Runtime:
    """Executor and plan builder factory pair used by parametrized tests.

    A `Runtime` is the smallest execution configuration supported by
    `WithDataFrame`. Tests normally use `Runtime.all()` to run with every
    supported local configuration, or `Runtime.minimal()` when a single runtime
    is sufficient.
    """

    __ALL: Optional[list["Runtime"]] = None
    __MINIMAL: Optional[list["Runtime"]] = None

    def __init__(self, executor: Executor, planFactory: PlanBuilderFactory):
        self.executor = executor
        self.planFactory = planFactory

    def __str__(self):
        return f"{self.executor}.{self.planFactory}"

    @staticmethod
    def minimal() -> list["Runtime"]:
        """Return the fastest runtime set for tests that do not need variants."""
        if Runtime.__MINIMAL is None:
            Runtime.__MINIMAL = [
                Runtime(LocalExecutor(), BoltPlanBuilderFactory()),
            ]
        return Runtime.__MINIMAL

    @staticmethod
    def all() -> list["Runtime"]:
        """Return every runtime covered by the common BoltML test suite.

        The base matrix is always ``LocalExecutor × {Bolt, Substrait}``. When
        the env var ``BOLTML_TEST_DISTRIBUTED`` is set to a truthy value
        (``1`` / ``true`` / ``yes`` / ``on``) and the ``ray`` dependency plus
        the boltml RayExecutor are importable, a third entry
        ``RayExecutor × Substrait`` is appended so every ``@WithDataFrame``-
        decorated test also exercises the distributed path.

        The Bolt plan-builder is *not* paired with the Ray executor because
        distributed execution is Substrait-only by design (see
        ``ray/remote_worker.py`` — it only knows how to deserialise a
        ``substraitPlanBytes`` payload).
        """
        if Runtime.__ALL is None:
            runtimes = [
                Runtime(LocalExecutor(), BoltPlanBuilderFactory()),
                Runtime(LocalExecutor(), SubstraitPlanBuilderFactory()),
            ]
            ray_runtime = RayRuntime.ifEnabled()
            if ray_runtime is not None:
                runtimes.append(ray_runtime)
            Runtime.__ALL = runtimes
        return Runtime.__ALL

    @staticmethod
    def executors(runtimes: list["Runtime"]) -> list[Executor]:
        """Return one executor instance per executor type from `runtimes`.

        This is useful for registering Python UDFs once per executor class while
        still deriving the set from the runtimes used by a parametrized test.
        """
        executors = []
        executor_types = set()
        for runtime in runtimes:
            executor_type = type(runtime.executor)
            if executor_type not in executor_types:
                executors.append(runtime.executor)
                executor_types.add(executor_type)
        return executors


class RayRuntime(Runtime):
    """A Ray-backed test :class:`Runtime` (``RayExecutor`` x Substrait).

    Distributed coverage is opt-in via ``BOLTML_TEST_DISTRIBUTED=1`` to keep
    fast pre-commit / IDE runs free of Ray's multi-second cluster init. The
    first ``RayExecutor`` construction is cached process-wide so the whole
    suite shares one cluster instead of paying init cost per file.
    """

    _cache: "Optional[RayRuntime]" = None
    _initialised: bool = False

    @classmethod
    def ifEnabled(cls) -> "Optional[Runtime]":
        """Return the cached Ray-backed runtime, or ``None`` when distributed
        coverage isn't opted into.

        Once ``BOLTML_TEST_DISTRIBUTED`` opts into distributed tests, ray
        import and ``RayExecutor`` construction errors are allowed to
        propagate (no ``try/except``) so CI fails on a missing or
        misconfigured Ray setup rather than silently falling back to
        local-only.
        """
        if cls._initialised:
            return cls._cache
        cls._initialised = True

        import os

        if os.environ.get("BOLTML_TEST_DISTRIBUTED", "").strip().lower() not in (
            "1",
            "true",
            "yes",
            "on",
        ):
            return None

        import ray  # noqa: F401

        from ..executor.ray import RayExecutor
        from ..distributed.ray import RayExecutionConfig, RayExecutionMode

        executor = RayExecutor(
            config=RayExecutionConfig(mode=RayExecutionMode.REMOTE_LEAF)
        )
        cls._cache = cls(executor, SubstraitPlanBuilderFactory())
        return cls._cache


class WithDataFrame:
    """Decorator for injecting DataFrames into test methods.

     This decorator enables parametric testing by running test methods with
     DataFrames created from each configured `Runtime`.

     **How It Works:**
     1. The decorator iterates over the configured runtimes.
     2. Before each runtime, the test fixture is reset with `tearDown()` followed
        by `setUp()` so runtime-specific DataFrames see the current fixture state.
     3. For each runtime, a DataFrame is created using the specified factory.
     4. The test method is invoked once per runtime with the injected DataFrame.

     **Usage:**
     ```python
    @WithDataFrame.data(c0=[1, 2, 3])
    def test_method(self, df):
        # df is a DataFrame with column 'c0' containing [1, 2, 3]
        # Executes for each runtime

    @WithDataFrame.random(c0=StringGenerator(), length=10)
    def test_random_data(self, df):
        # df is a DataFrame with 10 random string values in 'c0'
        # Executes for each runtime

     @WithDataFrame.factory(
         lambda plan_builder, executor: DataFrame(
             {"c0": [1, 2, 3]}, executor=executor, planFactory=plan_builder
         )
     )
     def test_runtime_data(self, df):
         # Build the DataFrame lazily for each runtime.
     ```

     **Test Coverage:**
     By default, each decorated test runs against `Runtime.all()`:
     - LocalExecutor with BoltPlanBuilderFactory
     - LocalExecutor with SubstraitPlanBuilderFactory

     This ensures comprehensive cross-compatibility testing.
    """

    def __init__(
        self,
        dataframeFactory: DataFrameFactory,
        runtimes: Optional[list[Runtime]] = None,
    ):
        self.__dataframeFactory = dataframeFactory
        self.__runtimes = runtimes or Runtime.all()

    @staticmethod
    def data(
        runtimes: Optional[list[Runtime]] = None,
        **data: dict[str, list],
    ) -> "WithDataFrame":
        """Create a decorator for tests using explicit data.

        This static method creates a decorator that injects DataFrames with
        explicitly provided data into test methods.

        Args:
            runtimes: Runtime configurations to test with. Defaults to
                `Runtime.all()`.
            **data: Keyword arguments mapping column names to their values.
                Values can be lists, arrays, or any iterable that DataFrame
                accepts.

        Returns:
            A WithDataFrame decorator that creates DataFrames from the provided data.
            The decorator will run the test once for each runtime.

        Example:
            @WithDataFrame.data(c0=[1, 2, 3], c1=["a", "b", "c"])
            def test_my_feature(self, df):
                # df has columns c0 and c1 with the provided values
                # Test runs once for each runtime
        """
        dataframeFactory = LambdaDataFrameFactory(
            lambda plan_builder, executor: DataFrame(
                data, executor=executor, planFactory=plan_builder
            )
        )
        return WithDataFrame(dataframeFactory, runtimes)

    @staticmethod
    def random(
        runtimes: Optional[list[Runtime]] = None,
        length: int = 4,
        **kwargs: dict[str, ColumnGenerator],
    ) -> "WithDataFrame":
        """Create a decorator for tests using randomly generated data.

        This static method creates a decorator that injects DataFrames with
        randomly generated data into test methods, enabling property-based
        testing and boundary condition testing. Each keyword argument defines a
        column name and the `ColumnGenerator` used to create that column.

        Args:
            runtimes: Runtime configurations to test with. Defaults to
                `Runtime.all()`.
            length: Number of rows to generate (default: 4). This controls
                the size of the generated DataFrame.
            **kwargs: Keyword arguments mapping column names to ColumnGenerator
                instances that define how to generate random data. Available
                generators include IntGenerator, StringGenerator, FloatGenerator,
                BoolGenerator, etc.

        Returns:
            A WithDataFrame decorator that creates DataFrames with random data.
            The decorator will run the test once for each runtime.

        Example:
            @WithDataFrame.random(c0=IntGenerator(), c1=StringGenerator(), length=10)
            def test_my_feature(self, df):
                # df has 10 rows with random ints in c0 and random strings in c1
                # Test runs once for each runtime
        """
        gen = RowVectorGenerator(**kwargs)
        dataframeFactory = RandomDataFrameFactory(gen, length)
        return WithDataFrame(dataframeFactory, runtimes)

    @staticmethod
    def dtype(
        dtype: RowType,
        runtimes: Optional[list[Runtime]] = None,
        length: int = 4,
    ) -> "WithDataFrame":
        """Create a decorator for tests using one random column of `dtype`.

        This is a convenience wrapper around `WithDataFrame.random()` for tests
        that need any dataframe of specific column types and names.

        Args:
            dtype: The dataframe data type.
            runtimes: Runtime configurations to test with. Defaults to
                `Runtime.all()`.
            length: Number of rows to generate (default: 4).

        Returns:
            A WithDataFrame decorator that creates DataFrames of the specified
            type for each runtime.

        Example:
            @WithDataFrame.dtype(RowType(["c0"], [IntegerType()], length=10)
            def test_integer_values(self, df):
                self.assertEqual(df["c0"].dtype, IntegerType())
        """
        gen = ColumnGenerator.from_dtype(dtype)
        dataframeFactory = RandomDataFrameFactory(gen, length)
        return WithDataFrame(dataframeFactory, runtimes)

    @staticmethod
    def factory(
        fn: Callable[[PlanBuilderFactory, Executor], DataFrame],
        runtimes: Optional[list[Runtime]] = None,
    ) -> "WithDataFrame":
        """Create a decorator from a custom DataFrame factory function.

        Use this when test data depends on per-runtime or per-fixture state and
        therefore cannot be captured safely when the decorator is defined. The
        function is called once per runtime after the test fixture has been reset
        for that runtime.

        Args:
            fn: Callable receiving `(plan_builder, executor)` and returning the
                DataFrame to pass to the test.
            runtimes: Runtime configurations to test with. Defaults to
                `Runtime.all()`.

        Returns:
            A WithDataFrame decorator that creates DataFrames by calling `fn`.

        Example:
            def test_with_fixture_data(self):
                @WithDataFrame.factory(
                    lambda plan_builder, executor: DataFrame(
                        {"url": [self._base_url]},
                        executor=executor,
                        planFactory=plan_builder,
                    )
                )
                def run(self, df):
                    ...

                run(self)
        """
        dataframeFactory = LambdaDataFrameFactory(fn)
        return WithDataFrame(dataframeFactory, runtimes)

    def __call__(self, fn: Callable) -> Callable:
        """Apply the decorator to a test method.

        The decorated test method will be called once for each runtime, with a
        DataFrame injected as the second argument.

        **Execution Flow:**
        1. Iterate over all configured runtimes.
        2. Reset the test fixture for the runtime with `tearDown()`/`setUp()`.
        3. Create a DataFrame with the configured `DataFrameFactory`.
        4. Invoke the original test method with the test instance and DataFrame.
        5. If any invocation raises an exception, the test fails.

        Args:
            fn: The test method to decorate. The method must accept two arguments:
                self (the test instance) and df (the injected DataFrame).

        Returns:
            A wrapped function that executes the test for each runtime.
            The wrapper preserves the original function's metadata using functools.wraps.
        """

        @wraps(fn)
        def wrapper(test, *args, **kwargs):
            for i, r in enumerate(self.__runtimes):
                with test.subTest(str(r)):
                    if i > 0:
                        test.tearDown()
                        test.setUp()
                    fn(
                        test,
                        self.__dataframeFactory.create(r.planFactory, r.executor),
                        *args,
                        **kwargs,
                    )

        return wrapper
