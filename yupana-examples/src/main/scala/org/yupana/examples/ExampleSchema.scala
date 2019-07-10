package org.yupana.examples

import org.yupana.api.schema.{Schema, Table}
import org.yupana.schema.SchemaRegistry

object ExampleSchema {
  val tables: Map[String, Table] = Seq(
    ExampleTables.itemsKkmTable,
    ExampleTables.kkmItemsTable,
    ExampleTables.receiptTable,
    ExampleTables.receiptByDayTable,
    ExampleTables.receiptByWeekTable,
    ExampleTables.receiptByMonthTable,
    ExampleTables.receiptByDayAllKkmsTable
  ).map(table => table.name -> table).toMap

  def schema: Schema = new Schema(tables, SchemaRegistry.defaultRollups)
}
