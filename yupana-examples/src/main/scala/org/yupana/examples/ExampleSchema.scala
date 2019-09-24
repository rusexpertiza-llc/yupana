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

import org.yupana.api.schema.{ Schema, Table }
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
