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

package org.yupana.core.providers

import org.yupana.api.query.{ Result, SimpleResult }
import org.yupana.api.schema._
import org.yupana.api.types.{ DataType, DataTypeMeta }
import org.yupana.core.ConstantCalculator
import org.yupana.core.sql.FunctionRegistry

import java.sql.DatabaseMetaData

class JdbcMetadataProvider(schema: Schema) {

  val functionRegistry = new FunctionRegistry(new ConstantCalculator(schema.tokenizer))

  private[providers] val columnFieldNames = List(
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
    "SCOPE_CATALOG",
    "SCOPE_SCHEMA",
    "SCOPE_TABLE",
    "IS_NULLABLE",
    "SOURCE_DATA_TYPE",
    "IS_AUTOINCREMENT",
    "IS_GENERATEDCOLUMN"
  )

  private[providers] val tableFieldNames = List("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS")

  def listTables: Result = {
    val data: Iterator[Array[Any]] = schema.tables.keys.map { name =>
      val desc = rollupDesc(name)
      Array[Any](null, null, name, if (desc.isEmpty) "TABLE" else "ROLLUP", desc.orNull)
    }.iterator
    SimpleResult("TABLES", tableFieldNames, tableFieldNames.map(_ => DataType[String]), data)
  }

  def describeTable(tableName: String): Either[String, Result] =
    schema.getTable(tableName) map { table =>
      val rollup = schema.rollups.find(_.toTable == table).collect { case t: TsdbRollup => t }

      val metricColumns = table.metrics.map { f =>
        val desc = rollup.flatMap(r => rollupMetricDesc(r, f))
        columnsArray(table.name, f.name, f.dataType.meta, nullable = DatabaseMetaData.columnNullable, desc)
      }

      val dimColumns = table.dimensionSeq.map { d =>
        val desc = rollup.flatMap(r => rollupDimDesc(r, d))
        columnsArray(table.name, d.name, d.dataType.meta, nullable = DatabaseMetaData.columnNoNulls, desc)
      }
      val timeColumn =
        columnsArray(
          table.name,
          "time",
          DataTypeMeta.timestampMeta,
          nullable = DatabaseMetaData.columnNullable,
          rollup.map(rollupTimeDesc)
        )

      val catalogColumns = table.externalLinks.flatMap(catalog => {
        catalog.fields.map(field =>
          columnsArray(
            table.name,
            catalog.linkName + "_" + field.name,
            field.dataType.meta,
            nullable = DatabaseMetaData.columnNullableUnknown,
            None
          )
        )
      })

      ((metricColumns :+ timeColumn) ++ dimColumns ++ catalogColumns).iterator
    } map { data =>
      SimpleResult(
        "COLUMNS",
        columnFieldNames,
        columnFieldNames.map(toColumnType),
        data
      )
    } toRight s"Unknown table '$tableName'"

  def listFunctions(typeName: String): Either[String, Result] = {
    DataType.bySqlName(typeName).map { t =>
      val fs = functionRegistry.functionsForType(t)
      SimpleResult("FUNCTIONS", Seq("NAME"), Seq(DataType[String]), fs.map(f => Array[Any](f)).iterator)
    } toRight s"Unknown type $typeName"
  }

  private def columnsArray[T](
      tableName: String,
      name: String,
      typeMeta: DataTypeMeta[T],
      nullable: Int,
      description: Option[String]
  ): Array[Any] = {
    columnsArray(tableName, name, typeMeta.sqlType, typeMeta.sqlTypeName, nullable, description)
  }

  private def columnsArray(
      tableName: String,
      name: String,
      sqlType: Int,
      typeName: String,
      nullable: Int,
      description: Option[String]
  ): Array[Any] = {
    val isoNullable = nullable match {
      case DatabaseMetaData.columnNullable => "YES"
      case DatabaseMetaData.columnNoNulls  => "NO"
      case _                               => ""
    }

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
      nullable,
      description.orNull,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      isoNullable,
      sqlType,
      "NO",
      "NO"
    )
  }

  private def toColumnType(column: String): DataType = column match {
    case "DATA_TYPE" | "SOURCE_DATA_TYPE" | "NULLABLE" => DataType[Int]
    case _                                             => DataType[String]
  }

  private def rollupDesc(tableName: String): Option[String] = {
    schema.rollups.find(_.toTable.name == tableName).map {
      case TsdbRollup(_, _, from, _, filter, _, groupBy) =>
        val where = filter.map(flt => s" WHERE $flt").getOrElse("")
        s"Rollup from $from$where GROUP BY ${groupBy.mkString(", ")}"
      case r =>
        s"Custom rollup from ${r.fromTable.name}"
    }
  }

  private def rollupTimeDesc(r: TsdbRollup): String = {
    r.timeExpr.toString
  }

  private def rollupMetricDesc(r: TsdbRollup, metric: Metric): Option[String] = {
    r.fields.collectFirst { case QueryFieldToMetric(f, m) if m == metric => f.expr.toString }
  }

  private def rollupDimDesc(r: TsdbRollup, dim: Dimension): Option[String] = {
    r.fields.collectFirst { case QueryFieldToDimension(f, d) if d == dim => f.expr.toString }
  }
}
