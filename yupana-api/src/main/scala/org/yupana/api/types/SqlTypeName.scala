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

import org.joda.time.Period
import org.yupana.api.{ Blob, Time }

/**
  * Mapping for Scala types to SQL type names
  * @param sqlTypeName SQL type name (like VARCHAR, INTEGER)
  */
case class SqlTypeName[T](sqlTypeName: String)

object SqlTypeName {

  implicit val boolMeta: SqlTypeName[Boolean] = SqlTypeName("BOOLEAN")
  implicit val stringMeta: SqlTypeName[String] = SqlTypeName("VARCHAR")
  implicit val byteMeta: SqlTypeName[Byte] = SqlTypeName("TINYINT")
  implicit val shortMeta: SqlTypeName[Short] = SqlTypeName("SMALLINT")
  implicit val intMeta: SqlTypeName[Int] = SqlTypeName("INTEGER")
  implicit val doubleMeta: SqlTypeName[Double] = SqlTypeName("DOUBLE")
  implicit val longMeta: SqlTypeName[Long] = SqlTypeName("BIGINT")
  implicit val decimalMeta: SqlTypeName[BigDecimal] = SqlTypeName("DECIMAL")
  implicit val timestampMeta: SqlTypeName[Time] = SqlTypeName("TIMESTAMP")
  implicit val periodMeta: SqlTypeName[Period] = SqlTypeName("PERIOD")

  implicit val nullMeta: SqlTypeName[Null] = SqlTypeName("NULL")

  implicit def seqName[T](implicit meta: SqlTypeName[T]): SqlTypeName[Seq[T]] =
    SqlTypeName(s"ARRAY[${meta.sqlTypeName}]")

  implicit val blobMeta: SqlTypeName[Blob] = SqlTypeName("BLOB")

  implicit def tuple[T, U](implicit tMeta: SqlTypeName[T], uMeta: SqlTypeName[U]): SqlTypeName[(T, U)] =
    SqlTypeName(s"${tMeta.sqlTypeName}_${uMeta.sqlTypeName}")
}
