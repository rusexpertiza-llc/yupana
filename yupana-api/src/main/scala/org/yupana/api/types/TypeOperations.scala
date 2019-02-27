package org.yupana.api.types

import org.joda.time.Period
import org.yupana.api.Time

case class TypeOperations[T](
  comparisons: Map[String, Comparison[T]],
  binaryOperations: Map[(String, String), BinaryOperation[T]],
  unaryOperations: Map[String, UnaryOperation[T]],
  aggregations: Map[String, Aggregation[T]]
) {
  def comparision(name: String): Option[Comparison[T]] = comparisons.get(name)
  def biOperation[U](name: String, argType: DataType.Aux[U]): Option[BinaryOperation.Aux[T, U, _]] = {
    binaryOperations.get((name, argType.meta.sqlTypeName))
      .map(op => op.asInstanceOf[BinaryOperation.Aux[T, U, op.Out]])
  }
  def unaryOperation(name: String): Option[UnaryOperation[T]] = unaryOperations.get(name)
  def aggregation(name: String): Option[Aggregation[T]] = aggregations.get(name)
}

object TypeOperations {
  def intOperations[T : Integral](dt: DataType.Aux[T]): TypeOperations[T] = TypeOperations(
    Comparison.ordComparisons,
    BinaryOperation.integralOperations(dt),
    UnaryOperation.numericOperations(dt),
    Aggregation.intAggregations(dt)
  )

  def fracOperations[T : Fractional](dt: DataType.Aux[T]): TypeOperations[T] = TypeOperations(
    Comparison.ordComparisons,
    BinaryOperation.fractionalOperations(dt),
    UnaryOperation.numericOperations(dt),
    Aggregation.fracAggregations(dt)
  )

  def stringOperations(dt: DataType.Aux[String]): TypeOperations[String] = TypeOperations(
    Comparison.ordComparisons,
    BinaryOperation.stringOperations,
    UnaryOperation.stringOperations,
    Aggregation.stringAggregations
  )

  def timeOperations(dt: DataType.Aux[Time]): TypeOperations[Time] = TypeOperations(
    Comparison.ordComparisons,
    BinaryOperation.timeOperations,
    UnaryOperation.timeOperations,
    Aggregation.timeAggregations
  )

  def periodOperations(dt: DataType.Aux[Period]): TypeOperations[Period] = TypeOperations(
    Map.empty,
    BinaryOperation.periodOperations,
    Map.empty,
    Map.empty
  )

  def tupleOperations[T, U](dtt: DataType.Aux[T], dtu: DataType.Aux[U]): TypeOperations[(T, U)] = {

    TypeOperations(
      Comparison.tupleComparisons(dtt.operations.comparisons, dtu.operations.comparisons),
      Map.empty,
      Map.empty,
      Map.empty
    )
  }
}
