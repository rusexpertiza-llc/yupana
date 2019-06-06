package org.yupana.api.query

import org.yupana.api.Time
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
  table: Table,
  fields: Seq[QueryField],
  filter: Condition,
  groupBy: Seq[Expression] = Seq.empty,
  limit: Option[Int] = None,
  postFilter: Option[Condition] = None
) {

  val uuid: String = hashCode().abs.toString
  val uuidLog: String = s"query_uuid: $uuid"

  override def toString: String = {
    val fs = fields.mkString("\n    ")

    val builder = StringBuilder.newBuilder
    builder.append(
    s"""Query(
         |  $uuidLog
         |  TABLE: ${table.name}
         |  FIELDS:
         |    $fs
         |""".stripMargin)

    builder.append(
      s"""  FILTER:
        |    $filter
        |""".stripMargin)

    if (groupBy.nonEmpty) {
      builder.append(
        s"""  GROUP BY: ${groupBy.mkString(",")}\n"""
      )
    }

    limit.foreach(l => builder.append(s"  LIMIT: $l\n"))
    postFilter.foreach { pf =>
      builder.append(s"  POSTFILTER: $pf")
    }

    builder.append(")")
    builder.toString
  }
}

object Query {
  def apply(table: Table,
            from: Expression.Aux[Time],
            to: Expression.Aux[Time],
            fields: Seq[QueryField],
            filter: Option[Condition],
            groupBy: Seq[Expression],
            limit: Option[Int],
            postFilter: Option[Condition]
           ): Query = {

    val newCondition = Condition.timeAndCondition(from, to, filter)
    new Query(table, fields, newCondition, groupBy, limit, postFilter)
  }

  def apply(table: Table,
            from: Expression.Aux[Time],
            to: Expression.Aux[Time],
            fields: Seq[QueryField]
           ): Query = apply(table, from, to, fields, None, Seq.empty, None, None)

  def apply(table: Table,
            from: Expression.Aux[Time],
            to: Expression.Aux[Time],
            fields: Seq[QueryField],
            filter: Condition
           ): Query = apply(table, from, to, fields, Some(filter), Seq.empty, None, None)

  def apply(table: Table,
            from: Expression.Aux[Time],
            to: Expression.Aux[Time],
            fields: Seq[QueryField],
            filter: Option[Condition],
            groupBy: Seq[Expression]
           ): Query = apply(table, from, to, fields, filter, groupBy, None, None)

}
