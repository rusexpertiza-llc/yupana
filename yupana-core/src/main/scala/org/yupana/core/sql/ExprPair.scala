/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.core.sql

import org.yupana.api.query._
import org.yupana.api.types.{ DataType, TypeConverter }
import org.yupana.core.ConstantCalculator

trait ExprPair {
  type T
  val a: Expression[T]
  val b: Expression[T]

  def dataType: DataType.Aux[T] = a.dataType
}

object ExprPair {
  def apply[T0](x: Expression[T0], y: Expression[T0]): ExprPair = new ExprPair {
    override type T = T0
    override val a: Expression[T0] = x
    override val b: Expression[T0] = y
  }

  def constCast[U, T](
      const: ConstantExpr[U],
      dataType: DataType.Aux[T],
      calc: ConstantCalculator
  ): Either[String, T] = {
    if (const.dataType == dataType) {
      Right(const.v.asInstanceOf[T])
    } else {
      safeConverter(const.dataType, dataType.aux)
        .map(conv => calc.evaluateConstant(conv(const)))
        .orElse(
          TypeConverter
            .partial(const.dataType, dataType.aux)
            .flatMap(conv => conv.convert(const.v))
        )
        .toRight(
          s"Cannot convert value '${const.v}' of type ${const.dataType.meta.sqlTypeName} to ${dataType.meta.sqlTypeName}"
        )
    }
  }

  def exprCast[U, T](
      e: Expression[U],
      dataType: DataType.Aux[T],
      calculator: ConstantCalculator
  ): Either[String, Expression[T]] = {
    if (e.dataType == dataType) Right(e.asInstanceOf[Expression[T]])
    else {
      e match {
        case c @ ConstantExpr(_, p) => constCast(c, dataType, calculator).map(v => ConstantExpr(v, p)(dataType))
        case _ =>
          safeConverter(e.dataType, dataType.aux)
            .orElse(unsafeConverter(e.dataType, dataType.aux))
            .map(conv => conv(e))
            .toRight(s"Cannot convert $e of type ${e.dataType} to $dataType")
      }
    }
  }

  def alignTypes[T, U](ca: Expression[T], cb: Expression[U], calc: ConstantCalculator): Either[String, ExprPair] = {
    if (ca.dataType == cb.dataType) {
      Right(ExprPair[T](ca, cb.asInstanceOf[Expression[T]]))
    } else {
      (ca, cb) match {
        case (_: ConstantExpr[_], _: ConstantExpr[_]) => convertRegular(ca, cb)

        case (c: ConstantExpr[_], _) =>
          constCast(c, cb.dataType, calc).map(cc => ExprPair(ConstantExpr(cc)(cb.dataType), cb))

        case (_, c: ConstantExpr[_]) =>
          constCast(c, ca.dataType, calc).map(cc => ExprPair(ca, ConstantExpr(cc)(ca.dataType)))

        case (_, _) => convertRegular(ca, cb)
      }
    }
  }

  private def convertRegular[T, U](ca: Expression[T], cb: Expression[U]): Either[String, ExprPair] = {
    safeConverter(ca.dataType, cb.dataType)
      .map(aToB => ExprPair[U](aToB(ca), cb))
      .orElse(
        safeConverter(cb.dataType, ca.dataType)
          .map(bToA => ExprPair[T](ca, bToA(cb)))
      )
      .toRight(s"Incompatible types ${ca.dataType.meta.sqlTypeName}($ca) and ${cb.dataType.meta.sqlTypeName}($cb)")
  }

  type ToTypeConverter[T, U] = Expression[T] => TypeConvertExpr[T, U]

  def safeConverter[T, U](implicit a: DataType.Aux[T], b: DataType.Aux[U]): Option[ToTypeConverter[T, U]] = {
    converters.get((a.meta.sqlTypeName, b.meta.sqlTypeName)).asInstanceOf[Option[ToTypeConverter[T, U]]]
  }

  def unsafeConverter[T, U](implicit a: DataType.Aux[T], b: DataType.Aux[U]): Option[ToTypeConverter[T, U]] = {
    if (b == DataType[String]) Some(((e: Expression[T]) => ToStringExpr(e)(a)).asInstanceOf[ToTypeConverter[T, U]])
    else
      unsafeConverters.get((a.meta.sqlTypeName, b.meta.sqlTypeName)).asInstanceOf[Option[ToTypeConverter[T, U]]]
  }

  private val converters: Map[(String, String), ToTypeConverter[_, _]] = Map(
    entry[Double, BigDecimal](Double2BigDecimalExpr),
    entry[Long, BigDecimal](Long2BigDecimalExpr),
    entry[Long, Double](Long2DoubleExpr),
    entry[Int, Long](Int2LongExpr),
    entry[Int, BigDecimal](Int2BigDecimalExpr),
    entry[Int, Double](Int2DoubleExpr),
    entry[Short, Int](Short2IntExpr),
    entry[Short, Long](Short2LongExpr),
    entry[Short, BigDecimal](Short2BigDecimalExpr),
    entry[Short, Double](Short2DoubleExpr),
    entry[Byte, Short](Byte2ShortExpr),
    entry[Byte, Int](Byte2IntExpr),
    entry[Byte, Long](Byte2LongExpr),
    entry[Byte, BigDecimal](Byte2BigDecimalExpr),
    entry[Byte, Double](Byte2DoubleExpr)
  )

  private val unsafeConverters: Map[(String, String), ToTypeConverter[_, _]] = Map()

  private def entry[T, U](ttc: ToTypeConverter[T, U])(
      implicit dtt: DataType.Aux[T],
      dtu: DataType.Aux[U]
  ): ((String, String), ToTypeConverter[_, _]) = {
    ((dtt.meta.sqlTypeName, dtu.meta.sqlTypeName), ttc)
  }
}
