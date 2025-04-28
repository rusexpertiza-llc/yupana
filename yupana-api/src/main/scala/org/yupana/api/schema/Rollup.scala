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

package org.yupana.api.schema

import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.query._

trait Rollup {
  val name: String
  val timeExpr: Expression[Time]
  val fromTable: Table
  val toTables: Seq[Table]
  val filter: Option[Condition]

  def withFromTable(table: Table): Rollup
  def withToTables(tables: Seq[Table]): Rollup
  def withName(newName: String): Rollup
}

/**
  * Definition of persistent rollup
  * @param name name of this rollup to be displayed
  * @param timeExpr time expression to group values
  * @param toTables tables to write data
  * @param fromTable table to read data
  * @param fields fields projections to be read from [[fromTable]] and written to [[toTables]]
  * @param filter condition to gather data
  * @param groupBy expressions to group by data
  */
case class TsdbRollup(
    override val name: String,
    override val timeExpr: Expression[Time],
    override val fromTable: Table,
    override val toTables: Seq[Table],
    override val filter: Option[Condition],
    fields: Seq[QueryFieldProjection],
    groupBy: Seq[Expression[_]]
) extends Rollup
    with Serializable {

  lazy val timeField: QueryField = timeExpr as Table.TIME_FIELD_NAME
  lazy val allFields: Seq[QueryFieldProjection] = QueryFieldToTime(timeField) +: fields
  lazy val allGroupBy: Seq[Expression[_]] = if (timeExpr != TimeExpr) timeExpr +: groupBy else groupBy

  override def withFromTable(table: Table): TsdbRollup = copy(fromTable = table)

  override def withToTables(tables: Seq[Table]): TsdbRollup = copy(toTables = tables)

  override def withName(newName: String): TsdbRollup = copy(name = newName)
}
