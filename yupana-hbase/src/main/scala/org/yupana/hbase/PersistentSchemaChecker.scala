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

package org.yupana.hbase

import org.yupana.api.Blob
import org.yupana.api.schema._
import org.yupana.api.types.DataType
import org.yupana.hbase.model.{ MetricInfo, SchemaInfo, TableInfo }

import java.nio.charset.StandardCharsets

object PersistentSchemaChecker extends SchemaChecker {

  private def typeMap: Map[String, String] =
    Map(
      DataType[Blob] -> DataType[Seq[Byte]]
    ).map { case (f, t) => f.meta.sqlTypeName -> t.meta.sqlTypeName }

  override def toBytes(schema: Schema): Array[Byte] =
    SchemaInfo(schema.tables.values.map(toInfo).toList).toJson.getBytes(StandardCharsets.UTF_8)

  override def check(schema: Schema, expectedSchemaBytes: Array[Byte]): SchemaCheckResult = {
    val expectedSchema = SchemaInfo.fromJson(new String(expectedSchemaBytes, StandardCharsets.UTF_8))
    val actualSchema = SchemaInfo(schema.tables.values.map(toInfo).toList)
    var checks = Seq.empty[SchemaCheckResult]
    if (actualSchema.tables.size != expectedSchema.tables.size) {
      checks :+= Warning(
        s"${expectedSchema.tables.size} tables expected, but ${actualSchema.tables.size} " +
          s"actually present in registry"
      )
    }
    checks ++= actualSchema.tables.map(verifyTags)
    checks ++= actualSchema.tables.map(t => {
      expectedSchema.tables.find(es => es.name == t.name) match {
        case None           => Warning(s"Unknown table ${t.name}")
        case Some(expected) => compareTables(t, expected)
      }
    })
    checks.fold(SchemaCheckResult.empty)(SchemaCheckResult.combine)
  }

  private def verifyTags(t: TableInfo): SchemaCheckResult = {
    t.metrics
      .groupBy(_.tag)
      .filter {
        case (tag, ms) => ms.size > 1
      }
      .map {
        case (tag, ms) =>
          Error(
            s"""In table ${t.name} ${ms.size} metrics (${ms.map(_.name).mkString(", ")}) share the same tag: $tag"""
          )
      }
      .fold(SchemaCheckResult.empty)(SchemaCheckResult.combine)
  }

  private def compareTables(a: TableInfo, e: TableInfo): SchemaCheckResult = {
    var checks = Seq(
      if (a.rowTimeSpan == e.rowTimeSpan)
        Success
      else Error(s"Expected rowTimeSpan for table ${a.name}: ${e.rowTimeSpan}, actual: ${a.rowTimeSpan}"),
      if (a.dimensions == e.dimensions)
        Success
      else
        Error(
          s"Expected dimensions for table ${a.name}: ${e.dimensions.mkString(", ")}; actual: ${a.dimensions.mkString(", ")}"
        )
    )

    val removedFields = e.metrics.filter(ef => !a.metrics.contains(ef))
    checks ++= removedFields.map(rf =>
      Error(s"In table ${a.name} metric ${rf.name}:${rf.sqlTypeName} has been removed or updated")
    )

    val unknownFields = a.metrics.filter(af => !e.metrics.contains(af))
    checks ++= unknownFields.map(uf =>
      Warning(s"In table ${a.name} metric ${uf.name}:${uf.sqlTypeName} is unknown (new)")
    )

    checks.fold(SchemaCheckResult.empty)(SchemaCheckResult.combine)
  }

  def toInfo(table: Table): TableInfo = {
    TableInfo(
      table.name,
      table.rowTimeSpan,
      table.dimensionSeq.map(_.name).toList,
      table.metrics.map(f => MetricInfo(f.name, f.tag, storageType(f.dataType.meta.sqlTypeName), f.group)).toList
    )
  }

  private def storageType(sqlType: String): String = {
    typeMap.getOrElse(sqlType, sqlType)
  }
}
