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

import org.yupana.api.schema._
import org.yupana.hbase.proto.{ Metric => ProtoMetric, SchemaRegistry => ProtoRegistry, Table => ProtoTable }

object ProtobufSchemaChecker extends SchemaChecker {
  override def toBytes(schema: Schema): Array[Byte] =
    new ProtoRegistry(schema.tables.values.map(asProto).toSeq).toByteArray

  override def check(schema: Schema, expectedSchemaBytes: Array[Byte]): SchemaCheckResult = {
    val expectedSchema = ProtoRegistry.parseFrom(expectedSchemaBytes)
    val actualSchema = new ProtoRegistry(schema.tables.values.map(asProto).toSeq)
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

  private def verifyTags(t: ProtoTable): SchemaCheckResult = {
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

  private def compareTables(a: ProtoTable, e: ProtoTable): SchemaCheckResult = {
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
    checks ++= removedFields.map(rf => Error(s"In table ${a.name} metric ${rf.name} has been removed or updated"))

    val unknownFields = a.metrics.filter(af => !e.metrics.contains(af))
    checks ++= unknownFields.map(uf => Warning(s"In table ${a.name} metric ${uf.name} is unknown (new)"))

    checks.fold(SchemaCheckResult.empty)(SchemaCheckResult.combine)
  }

  def asProto(table: Table): ProtoTable = {
    ProtoTable(
      table.name,
      table.rowTimeSpan,
      table.dimensionSeq.map(_.name),
      table.metrics.map(f => ProtoMetric(f.name, f.tag, f.dataType.meta.sqlTypeName, f.group))
    )
  }
}
