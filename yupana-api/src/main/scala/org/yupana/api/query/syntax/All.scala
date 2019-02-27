package org.yupana.api.query.syntax

trait All
  extends ExpressionSyntax
    with ConditionSyntax
    with DataTypeConverterSyntax
    with AggregationSyntax

object All extends All
