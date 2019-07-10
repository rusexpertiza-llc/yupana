package org.yupana.schema

import org.yupana.api.schema.{Rollup, Schema, Table}

object SchemaRegistry {
  val defaultTables: Map[String, Table] = Seq(
    Tables.itemsKkmTable,
    Tables.kkmItemsTable,
    Tables.receiptTable,
    Tables.receiptByDayTable,
    Tables.receiptByWeekTable,
    Tables.receiptByMonthTable,
    Tables.receiptByDayAllKkmsTable
  ).map(table => table.name -> table).toMap

  val defaultRollups: Seq[Rollup] = Seq(
    ReceiptRollups.receiptDayRollup,
    ReceiptRollups.receiptWeekRollup,
    ReceiptRollups.receiptMonthRollup,
    ReceiptRollups.receiptDayAllKkmsRollup
  )

  def defaultSchema: Schema = new Schema(defaultTables, defaultRollups)
}
