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

package org.yupana.postgres.protocol

import org.yupana.api.types.DataType

import java.sql.Types

object PgTypes {

  val PG_TYPE_VARCHAR = 1043
  val PG_TYPE_BOOL = 16
  val PG_TYPE_BYTEA = 17
  val PG_TYPE_BPCHAR = 1042
  val PG_TYPE_INT8 = 20
  val PG_TYPE_INT2 = 21
  val PG_TYPE_INT4 = 23
  val PG_TYPE_TEXT = 25
  val PG_TYPE_FLOAT4 = 700
  val PG_TYPE_FLOAT8 = 701
  val PG_TYPE_UNKNOWN = 705
  val PG_TYPE_INT2_ARRAY = 1005
  val PG_TYPE_INT4_ARRAY = 1007
  val PG_TYPE_VARCHAR_ARRAY = 1015
  val PG_TYPE_DATE = 1082
  val PG_TYPE_TIME = 1083
  val PG_TYPE_TIMETZ = 1266
  val PG_TYPE_TIMESTAMP = 1114
  val PG_TYPE_TIMESTAMPTZ = 1184
  val PG_TYPE_NUMERIC = 1700

  def pgForType(t: DataType): Int = {
    t.meta.sqlType match {
      case Types.VARCHAR   => PG_TYPE_VARCHAR
      case Types.BOOLEAN   => PG_TYPE_BOOL
      case Types.TINYINT   => PG_TYPE_NUMERIC
      case Types.SMALLINT  => PG_TYPE_INT2
      case Types.INTEGER   => PG_TYPE_INT4
      case Types.BIGINT    => PG_TYPE_INT8
      case Types.DOUBLE    => PG_TYPE_FLOAT8
      case Types.TIMESTAMP => PG_TYPE_TIMESTAMP
      case _               => PG_TYPE_UNKNOWN
    }
  }
}
