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

import org.yupana.api.Currency
import org.yupana.api.query._
import org.yupana.api.types.DataType
import org.yupana.core.ConstantCalculator

object DataTypeUtils {

  trait ExprPair {
    type T
    val a: Expression[T]
    val b: Expression[T]

    def dataType: DataType.Aux[T] = a.dataType
  }

  def pair[T0](x: Expression[T0], y: Expression[T0]): ExprPair = new ExprPair {
    override type T = T0
    override val a: Expression[T0] = x
    override val b: Expression[T0] = y
  }

  def constCast[U, T](
      v: U,
      fromType: DataType.Aux[U],
      toType: DataType.Aux[T],
      calc: ConstantCalculator
  ): Either[String, T] = {
    if (fromType == toType) {
      Right(v.asInstanceOf[T])
    } else {
      autoConverter(fromType, toType)
        .map(conv => calc.evaluateConstant(conv(ConstantExpr(v)(fromType))))
        .orElse(
          partial(fromType, toType)
            .flatMap(conv => conv(v))
        )
        .toRight(
          s"Cannot convert value '$v' of type ${fromType.meta.sqlTypeName} to ${toType.meta.sqlTypeName}"
        )
    }
  }

  def constCast[U, T](const: ConstExpr[U], dataType: DataType.Aux[T], calc: ConstantCalculator): Either[String, T] = {
    const match {
      case ConstantExpr(v) => constCast(v, const.dataType, dataType, calc)
      case NullExpr(_)     => Right(null.asInstanceOf[T])
      case TrueExpr =>
        if (dataType == DataType[Boolean]) Right(true.asInstanceOf[T])
        else Left(s"Cannot convert TRUE to data type $dataType")
      case FalseExpr =>
        if (dataType == DataType[Boolean]) Right(false.asInstanceOf[T])
        else Left(s"Cannot convert FALSE to data type $dataType")
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
        case c @ ConstantExpr(_) =>
          constCast(c, dataType, calculator)
            .orElse(
              manualConverter(e.dataType, dataType.aux)
                .map(conv => calculator.evaluateConstant[T](conv(e)))
                .toRight(s"Cannot convert $e of type ${e.dataType} to $dataType")
            )
            .map(x => ConstantExpr(x)(dataType.aux))
        case _ =>
          autoConverter(e.dataType, dataType.aux)
            .orElse(manualConverter(e.dataType, dataType.aux))
            .map(conv => conv(e))
            .toRight(s"Cannot convert $e of type ${e.dataType} to $dataType")
      }
    }
  }

  def alignConst[T, U](c: ConstExpr[T], t: DataType.Aux[U], calc: ConstantCalculator): Either[String, Expression[U]] = {
    constCast(c, t, calc).map(wrapConstant(_, t))
  }

  def alignTypes[T, U](ca: Expression[T], cb: Expression[U], calc: ConstantCalculator): Either[String, ExprPair] = {
    if (ca.dataType == cb.dataType) {
      Right(DataTypeUtils.pair[T](ca, cb.asInstanceOf[Expression[T]]))
    } else {
      (ca, cb) match {
        case (_: ConstantExpr[_], _: ConstantExpr[_]) => convertRegular(ca, cb)

        case (UntypedPlaceholderExpr(id), _) => Right(pair(PlaceholderExpr(id, cb.dataType.aux), cb))
        case (_, UntypedPlaceholderExpr(id)) => Right(pair(ca, PlaceholderExpr(id, ca.dataType.aux)))
        case (c: ConstExpr[_], _)            => alignConst(c, cb.dataType, calc).map(cc => DataTypeUtils.pair(cc, cb))
        case (_, c: ConstExpr[_])            => alignConst(c, ca.dataType, calc).map(cc => DataTypeUtils.pair(ca, cc))

        case (_, _) => convertRegular(ca, cb)
      }
    }
  }

  private def wrapConstant[T](v: T, dt: DataType.Aux[T]): ConstExpr[T] = {
    if (v != null) ConstantExpr(v)(dt) else NullExpr(dt)
  }

  private def convertRegular[T, U](ca: Expression[T], cb: Expression[U]): Either[String, ExprPair] = {
    autoConverter(ca.dataType, cb.dataType)
      .map(aToB => DataTypeUtils.pair[U](aToB(ca), cb))
      .orElse(
        autoConverter(cb.dataType, ca.dataType)
          .map(bToA => DataTypeUtils.pair[T](ca, bToA(cb)))
      )
      .toRight(s"Incompatible types ${ca.dataType}($ca) and ${cb.dataType}($cb)")
  }

  type ToTypeConverter[T, U] = Expression[T] => TypeConvertExpr[T, U]

  def autoConverter[T, U](implicit a: DataType.Aux[T], b: DataType.Aux[U]): Option[ToTypeConverter[T, U]] = {
    autoConverters.get((a.meta.sqlTypeName, b.meta.sqlTypeName)).asInstanceOf[Option[ToTypeConverter[T, U]]]
  }

  def manualConverter[T, U](implicit a: DataType.Aux[T], b: DataType.Aux[U]): Option[ToTypeConverter[T, U]] = {
    if (b == DataType[String]) Some(((e: Expression[T]) => ToStringExpr(e)(a)).asInstanceOf[ToTypeConverter[T, U]])
    else
      manualConverters.get((a.meta.sqlTypeName, b.meta.sqlTypeName)).asInstanceOf[Option[ToTypeConverter[T, U]]]
  }

  private val autoConverters: Map[(String, String), ToTypeConverter[_, _]] = Map(
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

  private val manualConverters: Map[(String, String), ToTypeConverter[_, _]] = Map()

  private def entry[T, U](ttc: ToTypeConverter[T, U])(
      implicit dtt: DataType.Aux[T],
      dtu: DataType.Aux[U]
  ): ((String, String), ToTypeConverter[_, _]) = {
    ((dtt.meta.sqlTypeName, dtu.meta.sqlTypeName), ttc)
  }

  type PartialConverter[T, U] = T => Option[U]

  def partial[T, U](implicit a: DataType.Aux[T], b: DataType.Aux[U]): Option[PartialConverter[T, U]] = {
    partials.get((a.meta.sqlTypeName, b.meta.sqlTypeName)).asInstanceOf[Option[PartialConverter[T, U]]]
  }

  private def pEntry[T, U](pc: PartialConverter[T, U])(
      implicit dtt: DataType.Aux[T],
      dtu: DataType.Aux[U]
  ): ((String, String), PartialConverter[T, U]) = {
    ((dtt.meta.sqlTypeName, dtu.meta.sqlTypeName), pc)
  }

  private val partials: Map[(String, String), PartialConverter[_, _]] = Map(
    pEntry[BigDecimal, Double](x => Some(x.toDouble)),
    pEntry[BigDecimal, Long](x => Option.when(x.isValidLong)(x.toLong)),
    pEntry[BigDecimal, Int](x => Option.when(x.isValidInt)(x.toInt)),
    pEntry[BigDecimal, Short](x => Option.when(x.isValidShort)(x.toShort)),
    pEntry[BigDecimal, Byte](x => Option.when(x.isValidByte)(x.toByte)),
    pEntry[BigDecimal, Currency] { x =>
      val bc = x * Currency.SUB
      Option.when(bc.isValidLong)(Currency(bc.toLong))
    }
  )
}
