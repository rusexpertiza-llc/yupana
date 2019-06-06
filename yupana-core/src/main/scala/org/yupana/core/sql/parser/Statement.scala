package org.yupana.core.sql.parser

sealed trait Statement

case class Select(
  schemaName: String,
  fields: SqlFields,
  condition: Option[Condition],
  groupings: Seq[SqlExpr],
  having: Option[Condition],
  limit: Option[Int]
) extends Statement

case object ShowTables extends Statement

case class ShowColumns(table: String) extends Statement
