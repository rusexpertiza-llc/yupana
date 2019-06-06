package org.yupana.hbase

import org.yupana.api.schema._
import org.yupana.hbase.proto.{Metric => ProtoMetric, SchemaRegistry => ProtoRegistry, Table => ProtoTable}

object ProtobufSchemaChecker extends SchemaChecker {
  override def toBytes(schema: Schema): Array[Byte] = new ProtoRegistry(schema.tables.values.map(asProto).toSeq).toByteArray

  override def check(schema: Schema, expectedSchemaBytes: Array[Byte]): SchemaCheckResult = {
    val expectedSchema = ProtoRegistry.parseFrom(expectedSchemaBytes)
    val actualSchema = new ProtoRegistry(schema.tables.values.map(asProto).toSeq)
    var checks = Seq.empty[SchemaCheckResult]
    if (actualSchema.tables.size != expectedSchema.tables.size) {
      checks :+= Warning(s"${expectedSchema.tables.size} tables expected, but ${actualSchema.tables.size} " +
        s"actually present in registry")
    }
    checks ++= actualSchema.tables.map(t => {
      expectedSchema.tables.find(es => es.name == t.name) match {
        case None => Warning(s"Unknown table ${t.name}")
        case Some(expected) => compareTables(t, expected)
      }
    })
    checks.fold(SchemaCheckResult.empty)(SchemaCheckResult.combine)
  }

  private def compareTables(a: ProtoTable, e: ProtoTable): SchemaCheckResult = {
    var checks = Seq(

      if (a.rowTimeSpan == e.rowTimeSpan)
        Success
      else Error(s"Expected rowTimeSpan for table ${a.name}: ${e.rowTimeSpan}, actual: ${a.rowTimeSpan}"),

      if (a.dimensions == e.dimensions)
        Success
      else Error(s"Expected dimensions for table ${a.name}: ${e.dimensions.mkString(", ")}; actual: ${a.dimensions.mkString(", ")}")
    )

    val removedFields = e.metrics.filter(ef => !a.metrics.contains(ef))
    checks ++= removedFields.map(rf => Error(s"In table ${a.name} metric ${rf.name} has been removed"))

    val unknownFields = a.metrics.filter(af => !e.metrics.contains(af))
    checks ++= unknownFields.map(uf => Warning(s"In table ${a.name} metric ${uf.name} is unknown (new)"))

    checks.fold(SchemaCheckResult.empty)(SchemaCheckResult.combine)
  }

  def asProto(table: Table): ProtoTable = {
    ProtoTable(table.name,
      table.rowTimeSpan,
      table.dimensionSeq.map(_.name),
      table.metrics.map(f => ProtoMetric(f.name, f.tag, f.dataType.meta.sqlTypeName, f.group))
    )
  }
}
