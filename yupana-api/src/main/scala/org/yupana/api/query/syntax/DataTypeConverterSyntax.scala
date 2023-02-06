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

package org.yupana.api.query.syntax

import org.yupana.api.query.{ Expression, TypeConvertExpr }
import org.yupana.api.types.TypeConverter

trait DataTypeConverterSyntax {
  def byte2BigDecimal(e: Expression[Byte]): Expression[BigDecimal] =
    convert[Byte, BigDecimal](e, TypeConverter.byte2BigDecimal)
  def byte2Double(e: Expression[Byte]): Expression[Double] = convert[Byte, Double](e, TypeConverter.byte2Double)
  def byte2Short(e: Expression[Byte]): Expression[Short] = convert[Byte, Short](e, TypeConverter.byte2Short)
  def byte2Int(e: Expression[Byte]): Expression[Int] = convert[Byte, Int](e, TypeConverter.byte2Int)
  def byte2Long(e: Expression[Byte]): Expression[Long] = convert[Byte, Long](e, TypeConverter.byte2Long)

  def short2BigDecimal(e: Expression[Short]): Expression[BigDecimal] =
    convert[Short, BigDecimal](e, TypeConverter.short2BigDecimal)
  def short2Double(e: Expression[Short]): Expression[Double] = convert[Short, Double](e, TypeConverter.short2Double)
  def short2Int(e: Expression[Short]): Expression[Int] = convert[Short, Int](e, TypeConverter.short2Int)
  def short2Long(e: Expression[Short]): Expression[Long] = convert[Short, Long](e, TypeConverter.short2Long)

  def int2bigDecimal(e: Expression[Int]): Expression[BigDecimal] =
    convert[Int, BigDecimal](e, TypeConverter.int2BigDecimal)
  def int2Double(e: Expression[Int]): Expression[Double] = convert[Int, Double](e, TypeConverter.int2Double)
  def int2Long(e: Expression[Int]): Expression[Long] = convert[Int, Long](e, TypeConverter.int2Long)

  def long2BigDecimal(e: Expression[Long]): Expression[BigDecimal] =
    convert[Long, BigDecimal](e, TypeConverter.long2BigDecimal)
  def long2Double(e: Expression[Long]): Expression[Double] = convert[Long, Double](e, TypeConverter.long2Double)

  def double2bigDecimal(e: Expression[Double]): Expression[BigDecimal] =
    convert[Double, BigDecimal](e, TypeConverter.double2BigDecimal)

  private def convert[T, U](e: Expression[T], typeConverter: TypeConverter[T, U]): TypeConvertExpr[T, U] =
    TypeConvertExpr(typeConverter, e)
}

object DataTypeConverterSyntax extends DataTypeConverterSyntax
