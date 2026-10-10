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

from typing import Optional

from substrait.proto import algebra

from pybolt import (
    BooleanType,
    TinyintType,
    SmallintType,
    IntegerType,
    BigintType,
    RealType,
    DoubleType,
    VarcharType,
    VarbinaryType,
)

from ..expression.base import Expression
from ..expression.cast import CastExpression
from ..expression.literal import LiteralExpression
from ..expression.field import FieldExpression
from ..expression.function import FunctionExpression
from ..expression.project import ProjectExpression
from ..expression.case import CaseExpression

from .types import substraitType
from .extensions import extensionRegister, extensionIndex


def substraitExpression(
    expression: Expression, rel: Optional[algebra.Rel] = None
) -> algebra.Expression:
    if not isinstance(expression, Expression):
        raise TypeError(f"Invalid expression type: {type(expression)}")

    if isinstance(expression, ProjectExpression):
        return substraitExpression(expression.inner(), rel)

    if isinstance(expression, FieldExpression):
        return fieldExpression(expression)

    if isinstance(expression, LiteralExpression) or expression.asLiteral() is not None:
        return literalExpression(expression.asLiteral())
    fn = expression.asFunction()
    if fn is not None:
        return vectorFunctionExpression(fn)
    if isinstance(expression, CastExpression):
        return castExpression(expression)

    if isinstance(expression, CaseExpression):
        return caseExpression(expression)

    raise NotImplementedError(
        "Conversion to substrait expression is not implemented for "
        f"expression type: {expression.__class__.__name__}."
    )


def literalExpression(expression: LiteralExpression) -> algebra.Expression:
    value = expression.value
    literal = algebra.Expression.Literal(nullable=True)
    if isinstance(expression.dtype, BooleanType):
        literal.boolean = value
    elif isinstance(expression.dtype, TinyintType):
        literal.i8 = value
    elif isinstance(expression.dtype, SmallintType):
        literal.i16 = value
    elif isinstance(expression.dtype, IntegerType):
        literal.i32 = value
    elif isinstance(expression.dtype, BigintType):
        literal.i64 = value
    elif isinstance(expression.dtype, RealType):
        literal.fp32 = value
    elif isinstance(expression.dtype, DoubleType):
        literal.fp64 = value
    elif isinstance(expression.dtype, VarcharType):
        literal.string = value
    elif isinstance(expression.dtype, VarbinaryType):
        literal.binary = value
    else:
        raise TypeError(
            f"Unsupported python literal of type {type(value)} to substrait literal."
        )
    return algebra.Expression(literal=literal)


def castExpression(expression: CastExpression) -> algebra.Expression:
    inputExpr = substraitExpression(expression.input())
    dtype = substraitType(expression.dtype)
    return algebra.Expression(
        cast=algebra.Expression.Cast(
            type=dtype,
            input=inputExpr,
            failure_behavior=algebra.Expression.Cast.FailureBehavior.FAILURE_BEHAVIOR_THROW_EXCEPTION,
        )
    )


def fieldExpression(expression: FieldExpression) -> algebra.Expression:
    return algebra.Expression(
        selection=algebra.Expression.FieldReference(
            direct_reference=algebra.Expression.ReferenceSegment(
                struct_field=algebra.Expression.ReferenceSegment.StructField(
                    field=expression.fieldIndex,
                )
            )
        )
    )


def vectorFunctionExpression(expression: FunctionExpression) -> algebra.Expression:
    ext = extensionRegister[expression]
    functionReference = extensionIndex(ext)
    arguments = [
        algebra.FunctionArgument(value=substraitExpression(arg))
        for arg in expression.arguments
    ]
    outputType = substraitType(expression.dtype)

    return algebra.Expression(
        scalar_function=algebra.Expression.ScalarFunction(
            function_reference=functionReference,
            arguments=arguments,
            output_type=outputType,
        )
    )


def caseExpression(expression: CaseExpression) -> algebra.Expression:
    if_then_expr = algebra.Expression.IfThen()

    for arm in expression.cases:
        if_expr = substraitExpression(arm.predicate)
        then_expr = substraitExpression(arm.value)
        if_clause = algebra.Expression.IfThen.IfClause(
            **{"if": if_expr, "then": then_expr}
        )
        if_then_expr.ifs.append(if_clause)

    if expression.defaultValue is not None:
        else_expr = substraitExpression(expression.defaultValue)
        if_then_expr = algebra.Expression.IfThen(
            **{"ifs": if_then_expr.ifs, "else": else_expr}
        )

    return algebra.Expression(if_then=if_then_expr)
