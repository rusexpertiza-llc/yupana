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

package org.yupana.api.types

import org.yupana.api.HexString

/**
  * Converter from type `In` to `Out`. Usually works with numeric types, increasing precision.
  *
  * @param dataType output data type
  * @param functionName name of this converter
  * @param convert conversion function from `In` to `Out`
  * @tparam In input type
  * @tparam Out output type
  */
class TypeConverter[In, Out](
    val dataType: DataType.Aux[Out],
    val functionName: String,
    val convert: In => Out
) extends Serializable

/**
  * Converter from type `In` to `Out` which might fail on some values. This can be used on numeric types decreasing size
  * (e.g. convert Long to Int if it possible).
  *
  * @param dataType output data type
  * @param functionName name of this converter
  * @param convert conversion function from `In` to `Out`
  * @tparam In input type
  * @tparam Out output type
  */
class PartialConverter[In, Out](
    val dataType: DataType.Aux[Out],
    val functionName: String,
    val convert: In => Option[Out]
)

object TypeConverter {

  /**
    * Look up for converter from `T` to `U`
    * @param a input data type
    * @param b output data type
    * @tparam T input type
    * @tparam U output type
    * @return a converter instance if available
    */
  def apply[T, U](implicit a: DataType.Aux[T], b: DataType.Aux[U]): Option[TypeConverter[T, U]] = {
    converters.get((a.meta.realSqlType, b.meta.realSqlType)).asInstanceOf[Option[TypeConverter[T, U]]]
  }

  /**
    * Look up for partial converter from `T` to `U`
    * @param a input data type
    * @param b output data type
    * @tparam T input type
    * @tparam U output type
    * @return a converter instance if available
    */
  def partial[T, U](implicit a: DataType.Aux[T], b: DataType.Aux[U]): Option[PartialConverter[T, U]] = {
    partials.get((a.meta.realSqlType, b.meta.realSqlType)).asInstanceOf[Option[PartialConverter[T, U]]]
  }

  val double2BigDecimal: TypeConverter[Double, BigDecimal] = mkTotal(x => BigDecimal(x))
  val long2BigDecimal: TypeConverter[Long, BigDecimal] = mkTotal(x => BigDecimal(x))
  val long2Double: TypeConverter[Long, Double] = mkTotal(_.toDouble)
  val int2Long: TypeConverter[Int, Long] = mkTotal(_.toLong)
  val int2BigDecimal: TypeConverter[Int, BigDecimal] = mkTotal(x => BigDecimal(x))
  val short2BigDecimal: TypeConverter[Short, BigDecimal] = mkTotal(x => BigDecimal(x))
  val byte2BigDecimal: TypeConverter[Byte, BigDecimal] = mkTotal(x => BigDecimal(x))

  val bigDecimal2Double: PartialConverter[BigDecimal, Double] = mkPartial(x => Some(x.toDouble))
  val bigDecimal2Long: PartialConverter[BigDecimal, Long] =
    mkPartial(x => if (x.isValidLong) Some(x.longValue) else None)
  val bigDecimal2Int: PartialConverter[BigDecimal, Int] = mkPartial(x => if (x.isValidInt) Some(x.toInt) else None)
  val bigDecimal2Short: PartialConverter[BigDecimal, Short] =
    mkPartial(x => if (x.isValidShort) Some(x.toShort) else None)
  val bigDecimal2Byte: PartialConverter[BigDecimal, Byte] = mkPartial(x => if (x.isValidByte) Some(x.toByte) else None)

  val string2Hex: PartialConverter[String, HexString] =
    mkPartial(x => if (HexString.isValidHex(x)) Some(HexString(x)) else None)

  def mkTotal[T, U](f: T => U)(
      implicit
      dtt: DataType.Aux[T],
      dtu: DataType.Aux[U]
  ): TypeConverter[T, U] = {
    new TypeConverter[T, U](
      dtu,
      dtt.meta.realSqlType.toLowerCase + "2" + dtu.meta.realSqlType.toLowerCase,
      f
    )
  }

  def mkPartial[T, U](
      f: T => Option[U]
  )(implicit dtt: DataType.Aux[T], dtu: DataType.Aux[U]): PartialConverter[T, U] = {
    new PartialConverter[T, U](
      dtu,
      dtt.meta.realSqlType.toLowerCase + "2" + dtu.meta.realSqlType.toLowerCase,
      f
    )
  }

  private def entry[T, U](tc: TypeConverter[T, U])(
      implicit
      dtt: DataType.Aux[T],
      dtu: DataType.Aux[U]
  ): ((String, String), TypeConverter[T, U]) = {
    ((dtt.meta.realSqlType, dtu.meta.realSqlType), tc)
  }

  private def pEntry[T, U](pc: PartialConverter[T, U])(
      implicit
      dtt: DataType.Aux[T],
      dtu: DataType.Aux[U]
  ): ((String, String), PartialConverter[T, U]) = {
    ((dtt.meta.realSqlType, dtu.meta.realSqlType), pc)
  }

  private val converters: Map[(String, String), TypeConverter[_, _]] = Map(
    entry[Double, BigDecimal](double2BigDecimal),
    entry[Long, BigDecimal](long2BigDecimal),
    entry[Long, Double](long2Double),
    entry[Int, Long](int2Long),
    entry[Int, BigDecimal](int2BigDecimal),
    entry[Short, BigDecimal](short2BigDecimal),
    entry[Byte, BigDecimal](byte2BigDecimal)
  )

  private val partials: Map[(String, String), PartialConverter[_, _]] = Map(
    pEntry[BigDecimal, Double](bigDecimal2Double),
    pEntry[BigDecimal, Long](bigDecimal2Long),
    pEntry[BigDecimal, Int](bigDecimal2Int),
    pEntry[BigDecimal, Short](bigDecimal2Short),
    pEntry[BigDecimal, Byte](bigDecimal2Byte),
    pEntry[String, HexString](string2Hex)
  )

}
