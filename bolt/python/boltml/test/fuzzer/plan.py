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

from typing import Union

from pybolt import JoinType, PlanBuilder, RowVector

from ...dataframe import DataFrame
from ...expression.named import NamedExpression
from ...plan_builder.base import PlanSource
from ...plan_builder.bolt import BoltPlanBuilder, BoltPlanBuilderFactory


def _plan_text(plan_builder: PlanBuilder):
    if hasattr(plan_builder, "boltPlan") and callable(plan_builder.boltPlan):
        plan_builder = plan_builder.boltPlan().plan()
    return plan_builder.planNode().toString(
        detailed=True,
        recursive=True,
        withId=False,
    )


def _assert_same_plan(test_case, dataframe: DataFrame, expected_plan: PlanBuilder):
    actual_plan = _plan_text(dataframe._planBuilder_)
    expected_plan_text = _plan_text(expected_plan)
    test_case.assertEqual(
        actual_plan,
        expected_plan_text,
        msg=f"\nactual plan:\n{actual_plan}\nexpected plan:\n{expected_plan_text}",
    )


def _base_plan(data: Union[dict, DataFrame, RowVector]):
    if isinstance(data, DataFrame):
        data = data._data_
    elif isinstance(data, dict):
        data = DataFrame(data)._data_
    return PlanBuilder().values([data])


def _raw_plan(plan_builder):
    if hasattr(plan_builder, "plan") and callable(plan_builder.plan):
        return plan_builder.plan()
    return plan_builder


def _rename_projection(
    names: list[str], replacements: dict[str, str], planBuilder: PlanBuilder
):
    if isinstance(planBuilder, PlanBuilder):
        df = DataFrame(
            BoltPlanBuilder(
                planBuilder, {}, PlanSource("empty", ()), BoltPlanBuilderFactory(), []
            )
        )
    else:
        df = DataFrame(planBuilder)
    return [
        NamedExpression(name, df[name])
        if name not in replacements
        else NamedExpression(replacements[name], df[name])
        for name in names
    ]


def _aggregation_sql(fn, alias: str, fallback_col: str):
    args = []
    for arg in fn.arguments:
        if hasattr(arg, "fieldName"):
            args.append(arg.fieldName)
        elif type(arg).__name__ == "AnyField":
            args.append(fallback_col)
        else:
            args.append(arg.expr())
    return f"{fn.name}({', '.join(args)}) as {alias}"


def _expected_join_plan_from_builders(
    lhs_names: list[str],
    rhs_names: list[str],
    lhs_plan: PlanBuilder,
    rhs_plan: PlanBuilder,
    join_key: str,
):
    lhs_plan = _raw_plan(lhs_plan)
    rhs_plan = _raw_plan(rhs_plan)
    keys = {join_key}

    lhs_name_map = {name: f"l_{name}" for name in lhs_names if name not in keys}
    rhs_name_map = {name: f"r_{name}" for name in rhs_names if name not in keys}
    lhs_keys = {key: f"l_{key}" for key in keys}
    rhs_keys = {key: f"r_{key}" for key in keys}

    lhs_proj = _rename_projection(lhs_names, lhs_name_map | lhs_keys, lhs_plan)
    rhs_proj = _rename_projection(rhs_names, rhs_name_map | rhs_keys, rhs_plan)
    if isinstance(lhs_plan, PlanBuilder):
        lhs_proj = [proj.expr() for proj in lhs_proj]
    if isinstance(rhs_plan, PlanBuilder):
        rhs_proj = [proj.expr() for proj in rhs_proj]
    lhs_plan.project(lhs_proj)
    rhs_plan.project(rhs_proj)

    output_layout = (
        list(lhs_keys.values())
        + list(lhs_name_map.values())
        + list(rhs_name_map.values())
    )

    if isinstance(lhs_plan, PlanBuilder):
        if isinstance(rhs_plan, PlanBuilder):
            planNode = rhs_plan.planNode()
        else:
            planNode = rhs_plan.boltPlan().plan().planNode()
        lhs_plan.hashJoin(
            list(lhs_keys.values()),
            list(rhs_keys.values()),
            planNode,
            outputLayout=output_layout,
            joinType=JoinType.kInner,
            nullAware=False,
        )
    else:
        lhs_plan.join(
            rhs_plan,
            [DataFrame(lhs_plan)[k] for k in lhs_keys.values()],
            [DataFrame(rhs_plan)[k] for k in rhs_keys.values()],
            outputLayout=output_layout,
            joinType=JoinType.kInner,
        )

    restored_names = {value: key for key, value in lhs_keys.items()}
    restored_names |= {
        value: key for key, value in lhs_name_map.items() if key not in rhs_name_map
    }
    restored_names |= {
        value: key for key, value in rhs_name_map.items() if key not in lhs_name_map
    }

    lhs_proj = _rename_projection(output_layout, restored_names, lhs_plan)
    if isinstance(lhs_plan, PlanBuilder):
        lhs_proj = [proj.expr() for proj in lhs_proj]
    lhs_plan.project(lhs_proj)
    return lhs_plan


def _expected_join_plan(lhs_data: dict, rhs_data: dict, join_key: str):
    lhs_df = DataFrame(lhs_data)
    rhs_df = DataFrame(rhs_data)
    return _expected_join_plan_from_builders(
        lhs_df.names,
        rhs_df.names,
        lhs_df._planBuilder_,
        rhs_df._planBuilder_,
        join_key,
    )


def _expected_join_plan_from_frames(
    lhs_df: DataFrame, rhs_df: DataFrame, join_key: str
):
    return _expected_join_plan_from_builders(
        lhs_df.names,
        rhs_df.names,
        DataFrame(lhs_df._data_, planFactory=lhs_df._planFactory_)._planBuilder_,
        DataFrame(rhs_df._data_, planFactory=rhs_df._planFactory_)._planBuilder_,
        join_key,
    )
