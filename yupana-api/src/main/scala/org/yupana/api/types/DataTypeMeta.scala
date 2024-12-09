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

import org.threeten.extra.PeriodDuration

import java.sql.Types
import org.yupana.api.{ Blob, Currency, Time }

/**
  * Contains different meta information for type `T`
  * @param sqlType SQL type from `java.sql.Types`
  * @param displaySize fiels of this type size in characters
  * @param sqlTypeName SQL type name (like VARCHAR, INTEGER)
  * @param javaTypeName Java type of this data type
  * @param precision field precision for numeric types
  * @param isSigned specifies if the numeric type signed
  * @param scale scale for numeric types
  * @tparam T real scala type
  */
case class DataTypeMeta[T](
    sqlType: Int,
    displaySize: Int,
    sqlTypeName: String,
    javaTypeName: String,
    precision: Int,
    isSigned: Boolean,
    scale: Int
)

object DataTypeMeta {

  val MONEY_SCALE = 2
  val MAX_PRECISION = 38 // Same as Spark

  private val SIGNED_TYPES =
    Set(Types.INTEGER, Types.BIGINT, Types.DOUBLE, Types.DECIMAL, Types.SMALLINT, Types.TINYINT)

  implicit val boolMeta: DataTypeMeta[Boolean] =
    DataTypeMeta(Types.BOOLEAN, 5, "BOOLEAN", classOf[java.lang.Boolean], 0, 0)
  implicit val stringMeta: DataTypeMeta[String] =
    DataTypeMeta(Types.VARCHAR, Integer.MAX_VALUE, "VARCHAR", classOf[java.lang.String], Integer.MAX_VALUE, 0)
  implicit val byteMeta: DataTypeMeta[Byte] =
    DataTypeMeta(Types.TINYINT, 3, "TINYINT", classOf[java.lang.Byte], 3, 0)
  implicit val shortMeta: DataTypeMeta[Short] =
    DataTypeMeta(Types.SMALLINT, 5, "SMALLINT", classOf[java.lang.Short], 5, 0)
  implicit val intMeta: DataTypeMeta[Int] =
    DataTypeMeta(Types.INTEGER, 10, "INTEGER", classOf[java.lang.Integer], 10, 0)
  implicit val doubleMeta: DataTypeMeta[Double] =
    DataTypeMeta(Types.DOUBLE, 25, "DOUBLE", classOf[java.lang.Double], 17, 17)
  implicit val longMeta: DataTypeMeta[Long] = DataTypeMeta(Types.BIGINT, 20, "BIGINT", classOf[java.lang.Long], 19, 0)
  implicit val decimalMeta: DataTypeMeta[BigDecimal] = scaledDecimalMeta(MONEY_SCALE)
  implicit val currencyMeta: DataTypeMeta[Currency] =
    DataTypeMeta(Types.DECIMAL, 20, "CURRENCY", classOf[java.lang.Long], 19, 2)
  implicit val timestampMeta: DataTypeMeta[Time] =
    DataTypeMeta(Types.TIMESTAMP, 23, "TIMESTAMP", classOf[java.sql.Timestamp], 23, 6)
  implicit val periodMeta: DataTypeMeta[PeriodDuration] =
    DataTypeMeta(Types.VARCHAR, 20, "PERIOD", classOf[java.lang.String], 20, 0)

  implicit val nullMeta: DataTypeMeta[Null] = DataTypeMeta(Types.NULL, 4, "NULL", null, 0, 0)

  implicit def seqMeta[T](implicit meta: DataTypeMeta[T]): DataTypeMeta[Seq[T]] = {
    DataTypeMeta(
      Types.ARRAY,
      Integer.MAX_VALUE,
      s"ARRAY[${meta.sqlTypeName}]",
      classOf[java.lang.Object],
      Integer.MAX_VALUE,
      0
    )
  }

  implicit val blobMeta: DataTypeMeta[Blob] =
    DataTypeMeta(Types.BLOB, Int.MaxValue, "BLOB", classOf[java.sql.Blob], Int.MaxValue, 0)

  def scaledDecimalMeta(scale: Int): DataTypeMeta[BigDecimal] = {
    DataTypeMeta(Types.DECIMAL, 131089, "DECIMAL", classOf[java.math.BigDecimal], MAX_PRECISION, scale)
  }

  def apply[T](t: Int, ds: Int, tn: String, jt: Class[_], p: Int, s: Int): DataTypeMeta[T] =
    DataTypeMeta(t, ds, tn, if (jt != null) jt.getCanonicalName else "Null", p, SIGNED_TYPES.contains(t), s)

  def tuple[T, U](implicit tMeta: DataTypeMeta[T], uMeta: DataTypeMeta[U]): DataTypeMeta[(T, U)] = DataTypeMeta(
    Types.OTHER,
    tMeta.displaySize + uMeta.displaySize,
    s"${tMeta.sqlTypeName}_${uMeta.sqlTypeName}",
    classOf[(T, U)].getCanonicalName,
    tMeta.precision + uMeta.precision,
    isSigned = false,
    0
  )
}
