package org.yupana.api.query.syntax

trait All
  extends ExpressionSyntax
    with ConditionSyntax
    with DataTypeConverterSyntax
    with AggregationSyntax
    with UnaryOperationSyntax
    with BinaryOperationSyntax

object All extends All
