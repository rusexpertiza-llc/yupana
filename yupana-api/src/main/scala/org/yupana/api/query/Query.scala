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

package org.yupana.api.query

import java.util.UUID

import org.yupana.api.Time
import org.yupana.api.query.Expression.Condition
import org.yupana.api.schema.Table

/**
  * Query to TSDB
  *
  * @param table table to query data
  * @param fields set of fields to be calculated
  * @param filter primary data filter
  * @param groupBy groupings
  * @param limit a number of records to be extracted
  * @param postFilter filter applied after aggregation stage (HAVING statement in SQL).
  */
case class Query(
    table: Option[Table],
    fields: Seq[QueryField],
    filter: Option[Condition],
    groupBy: Seq[Expression[_]] = Seq.empty,
    limit: Option[Int] = None,
    postFilter: Option[Condition] = None
) {

  val id: String = System.nanoTime().toString + UUID.randomUUID().toString
  val uuidLog: String = s"query_id: $id"

  override def toString: String = {
    val fs = fields.mkString("\n    ")

    val builder = new StringBuilder()
    builder.append(s"""Query(
         |  $uuidLog
         |  FIELDS:
         |    $fs
         |""".stripMargin)

    table.foreach { t =>
      builder.append(s"  FROM: ${t.name}")
    }

    filter.foreach { f =>
      builder.append(s"""
        |  FILTER:
        |    $f
        |""".stripMargin)
    }

    if (groupBy.nonEmpty) {
      builder.append(
        s"""  GROUP BY: ${groupBy.mkString(", ")}\n"""
      )
    }

    limit.foreach(l => builder.append(s"  LIMIT: $l\n"))
    postFilter.foreach { pf =>
      builder.append(s"""  POSTFILTER:
           |    $pf
           |""".stripMargin)
    }

    builder.append(")")
    builder.toString
  }
}

object Query {
  def apply(
      table: Table,
      from: Expression[Time],
      to: Expression[Time],
      fields: Seq[QueryField],
      filter: Option[Condition],
      groupBy: Seq[Expression[_]],
      limit: Option[Int],
      postFilter: Option[Condition]
  ): Query = {

    val newCondition = AndExpr(
      Seq(
        GeExpr(TimeExpr, from),
        LtExpr(TimeExpr, to)
      ) ++ filter
    )

    new Query(Some(table), fields, Some(newCondition), groupBy, limit, postFilter)
  }

  def apply(table: Table, from: Expression[Time], to: Expression[Time], fields: Seq[QueryField]): Query =
    apply(table, from, to, fields, None, Seq.empty, None, None)

  def apply(
      table: Table,
      from: Expression[Time],
      to: Expression[Time],
      fields: Seq[QueryField],
      filter: Condition
  ): Query = apply(table, from, to, fields, Some(filter), Seq.empty, None, None)

  def apply(
      table: Table,
      from: Expression[Time],
      to: Expression[Time],
      fields: Seq[QueryField],
      filter: Option[Condition],
      groupBy: Seq[Expression[_]]
  ): Query = apply(table, from, to, fields, filter, groupBy, None, None)

}
