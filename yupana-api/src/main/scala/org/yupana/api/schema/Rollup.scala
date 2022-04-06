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
  val toTable: Table
}

/**
  * Definition of persistent rollup
  * @param name name of this rollup to be displayed
  * @param timeExpr time expression to group values
  * @param toTable table to write data
  * @param fromTable table to read data
  * @param fields fields projections to be read from [[fromTable]] and written to [[toTable]]
  * @param filter condition to gather data
  * @param groupBy expressions to group by data
  */
case class TsdbRollup(
    override val name: String,
    override val timeExpr: Expression[Time],
    override val fromTable: Table,
    override val toTable: Table,
    fields: Seq[QueryFieldProjection],
    filter: Option[Condition],
    groupBy: Seq[Expression[_]]
) extends Rollup
    with Serializable {

  lazy val timeField: QueryField = timeExpr as Table.TIME_FIELD_NAME
  lazy val allFields: Seq[QueryFieldProjection] = QueryFieldToTime(timeField) +: fields
  lazy val allGroupBy: Seq[Expression[_]] = if (timeExpr != TimeExpr) timeExpr +: groupBy else groupBy

  lazy val tagResultNameMap: Map[String, String] = allFields.collect {
    case QueryFieldToDimension(queryField, dimension) =>
      dimension.name -> queryField.name
  }.toMap

  lazy val fieldNamesMap: Map[String, String] = allFields.collect {
    case QueryFieldToMetric(queryField, field) =>
      field.name -> queryField.name
  }.toMap

  def getResultFieldForDimName(dimName: String): String = {
    tagResultNameMap.getOrElse(dimName, throw new Exception(s"Can't find result field for tag name: $dimName"))
  }

  def getResultFieldForMeasureName(fieldName: String): String = {
    fieldNamesMap.getOrElse(fieldName, throw new Exception(s"Can't find result field for field name: $fieldName"))
  }
}
