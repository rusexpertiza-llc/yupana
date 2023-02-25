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

import org.yupana.api.query._

trait DataTypeConverterSyntax {
  def byte2BigDecimal(e: Expression[Byte]): Expression[BigDecimal] = Byte2BigDecimalExpr(e)
  def byte2Double(e: Expression[Byte]): Expression[Double] = Byte2DoubleExpr(e)
  def byte2Short(e: Expression[Byte]): Expression[Short] = Byte2ShortExpr(e)
  def byte2Int(e: Expression[Byte]): Expression[Int] = Byte2IntExpr(e)
  def byte2Long(e: Expression[Byte]): Expression[Long] = Byte2LongExpr(e)

  def short2BigDecimal(e: Expression[Short]): Expression[BigDecimal] = Short2BigDecimalExpr(e)
  def short2Double(e: Expression[Short]): Expression[Double] = Short2DoubleExpr(e)
  def short2Int(e: Expression[Short]): Expression[Int] = Short2IntExpr(e)
  def short2Long(e: Expression[Short]): Expression[Long] = Short2LongExpr(e)

  def int2bigDecimal(e: Expression[Int]): Expression[BigDecimal] = Int2BigDecimalExpr(e)
  def int2Double(e: Expression[Int]): Expression[Double] = Int2DoubleExpr(e)
  def int2Long(e: Expression[Int]): Expression[Long] = Int2LongExpr(e)

  def long2BigDecimal(e: Expression[Long]): Expression[BigDecimal] = Long2BigDecimalExpr(e)
  def long2Double(e: Expression[Long]): Expression[Double] = Long2DoubleExpr(e)

  def double2bigDecimal(e: Expression[Double]): Expression[BigDecimal] = Double2BigDecimalExpr(e)
}

object DataTypeConverterSyntax extends DataTypeConverterSyntax
