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

package org.yupana.examples

import org.yupana.api.schema.Table
import org.yupana.examples.externallinks.{ AddressCatalog, GeoCatalog, OrganisationCatalog }
import org.yupana.schema.Tables

object ExampleTables {
  private val extraLinks = Seq(AddressCatalog, OrganisationCatalog, GeoCatalog)

  val itemsKkmTable: Table = Tables.itemsKkmTable.withExternalLinks(extraLinks)
  val kkmItemsTable: Table = Tables.kkmItemsTable.withExternalLinks(extraLinks)

  val receiptTable: Table = Tables.receiptTable.withExternalLinks(extraLinks)
  val receiptByDayTable: Table = Tables.receiptByDayTable.withExternalLinks(extraLinks)
  val receiptByWeekTable: Table = Tables.receiptByWeekTable.withExternalLinks(extraLinks)
  val receiptByMonthTable: Table = Tables.receiptByMonthTable.withExternalLinks(extraLinks)
  val receiptByDayAllKkmsTable: Table = Tables.receiptByDayAllKkmsTable.withExternalLinks(extraLinks)
}
