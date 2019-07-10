package org.yupana.examples

import org.yupana.api.schema.Table
import org.yupana.examples.externallinks.{AddressCatalog, OrganisationCatalog}
import org.yupana.schema.Tables

object ExampleTables {
  private val extraLinks = Seq(AddressCatalog, OrganisationCatalog)

  val itemsKkmTable: Table = Tables.itemsKkmTable.withExternalLinks(extraLinks)
  val kkmItemsTable: Table = Tables.kkmItemsTable.withExternalLinks(extraLinks)

  val receiptTable: Table = Tables.receiptTable.withExternalLinks(extraLinks)
  val receiptByDayTable: Table = Tables.receiptByDayTable.withExternalLinks(extraLinks)
  val receiptByWeekTable: Table = Tables.receiptByWeekTable.withExternalLinks(extraLinks)
  val receiptByMonthTable: Table = Tables.receiptByMonthTable.withExternalLinks(extraLinks)
  val receiptByDayAllKkmsTable: Table = Tables.receiptByDayAllKkmsTable.withExternalLinks(extraLinks)
}
