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

package org.yupana.akka

import org.yupana.api.query.{ Result, SimpleResult }
import org.yupana.api.schema.Schema
import org.yupana.api.types.{ DataType, DataTypeMeta }

class JdbcMetadataProvider(schema: Schema) {

  val columnFieldNames = List(
    "TABLE_CAT",
    "TABLE_SCHEM",
    "TABLE_NAME",
    "COLUMN_NAME",
    "DATA_TYPE",
    "TYPE_NAME",
    "COLUMN_SIZE",
    "BUFFER_LENGTH",
    "DECIMAL_DIGITS",
    "NUM_PREC_RADIX",
    "NULLABLE",
    "REMARKS",
    "COLUMN_DEF",
    "SQL_DATA_TYPE",
    "SQL_DATETIME_SUB",
    "CHAR_OCTET_LENGTH",
    "ORDINAL_POSITION",
    "IS_NULLABLE"
  )

  val tableFieldNames = List("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS")

  def listTables: Result = {
    val data: Iterator[Array[Any]] = schema.tables.keys.map { name =>
      Array[Any](null, null, name, "TABLE", null)
    }.iterator
    SimpleResult("TABLES", tableFieldNames, tableFieldNames.map(_ => DataType[String]), data)
  }

  def describeTable(tableName: String): Either[String, Result] =
    schema.getTable(tableName) map { table =>
      val metricColumns = table.metrics.map { f =>
        columnsArray(table.name, f.name, f.dataType.meta)
      }

      val dimColumns = table.dimensionSeq.map(d => columnsArray(table.name, d.name, d.dataType.meta))
      val timeColumn = columnsArray(table.name, "time", DataTypeMeta.timestampMeta)

      val catalogColumns = table.externalLinks.flatMap(catalog => {
        catalog.fields.map(field => columnsArray(table.name, catalog.linkName + "_" + field.name, field.dataType.meta))
      })

      ((metricColumns :+ timeColumn) ++ dimColumns ++ catalogColumns).iterator
    } map { data =>
      SimpleResult(
        "COLUMNS",
        columnFieldNames,
        columnFieldNames.map(n => if (n == "DATA_TYPE") DataType[Int] else DataType[String]),
        data
      )
    } toRight s"Unknown schema '$tableName'"

  private def columnsArray[T](tableName: String, name: String, typeMeta: DataTypeMeta[T]): Array[Any] = {
    columnsArray(tableName, name, typeMeta.sqlType, typeMeta.sqlTypeName)
  }

  private def columnsArray(tableName: String, name: String, sqlType: Int, typeName: String): Array[Any] = {
    Array[Any](
      null,
      null,
      tableName,
      name,
      sqlType,
      typeName,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null
    )
  }
}
