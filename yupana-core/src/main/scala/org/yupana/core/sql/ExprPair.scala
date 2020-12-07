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

import org.yupana.api.query.{ ConstantExpr, Expression, TypeConvertExpr }
import org.yupana.api.types.{ DataType, TypeConverter }

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

  def constCast[U, T](const: ConstantExpr[U], dataType: DataType.Aux[T]): Either[String, T] = {
    if (const.dataType == dataType) {
      Right(const.v.asInstanceOf[T])
    } else {
      TypeConverter(const.dataType, dataType.aux)
        .map(conv => conv.convert(const.v))
        .orElse(
          TypeConverter
            .partial(const.dataType, dataType.aux)
            .flatMap(conv => conv.convert(const.v))
        )
        .toRight(
          s"Cannot convert value '${const.v}' of type ${const.dataType.sqlTypeName} to ${dataType.sqlTypeName}"
        )
    }
  }

  def alignTypes[T, U](ca: Expression[T], cb: Expression[U]): Either[String, ExprPair] = {
    if (ca.dataType == cb.dataType) {
      Right(ExprPair[T](ca, cb.asInstanceOf[Expression[T]]))
    } else {
      (ca, cb) match {
        case (_: ConstantExpr[_], _: ConstantExpr[_]) => convertRegular(ca, cb)

        case (c: ConstantExpr[_], _) =>
          constCast(c, cb.dataType).right.map(cc => ExprPair(ConstantExpr(cc)(cb.dataType), cb))

        case (_, c: ConstantExpr[_]) =>
          constCast(c, ca.dataType).right.map(cc => ExprPair(ca, ConstantExpr(cc)(ca.dataType)))

        case (_, _) => convertRegular(ca, cb)
      }
    }
  }

  private def convertRegular[T, U](ca: Expression[T], cb: Expression[U]): Either[String, ExprPair] = {
    TypeConverter(ca.dataType, cb.dataType)
      .map(aToB => ExprPair[U](TypeConvertExpr(aToB, ca), cb))
      .orElse(
        TypeConverter(cb.dataType, ca.dataType)
          .map(bToA => ExprPair[T](ca, TypeConvertExpr(bToA, cb)))
      )
      .toRight(s"Incompatible types ${ca.dataType.sqlTypeName}($ca) and ${cb.dataType.sqlTypeName}($cb)")
  }
}
