package org.yupana.api.query.syntax

import org.yupana.api.query.{Expression, FunctionExpr}
import org.yupana.api.types.TypeConverter

trait DataTypeConverterSyntax {
  def double2bigDecimal(e: Expression.Aux[Double]): Expression.Aux[BigDecimal] = convert[Double, BigDecimal](e, TypeConverter.double2BigDecimal)
  def long2BigDecimal(e: Expression.Aux[Long]): Expression.Aux[BigDecimal]= convert[Long, BigDecimal](e, TypeConverter.long2BigDecimal)
  def long2Double(e: Expression.Aux[Long]): Expression.Aux[Double] = convert[Long, Double](e, TypeConverter.long2Double)
  def int2Long(e: Expression.Aux[Int]): Expression.Aux[Long] = convert[Int, Long](e, TypeConverter.int2Long)
  def int2bigDecimal(e: Expression.Aux[Int]): Expression.Aux[BigDecimal] = convert[Int, BigDecimal](e, TypeConverter.int2BigDecimal)

  private def convert[T, U](e: Expression.Aux[T], typeConverter: TypeConverter[T, U]): FunctionExpr[T, U] = FunctionExpr(typeConverter, e)
}

object DataTypeConverterSyntax extends DataTypeConverterSyntax
