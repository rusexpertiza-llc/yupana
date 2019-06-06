package org.yupana.core.sql.parser

case class SqlField(expr: SqlExpr, alias: Option[String] = None)

object LowercaseFieldName {
  def unapply(fn: FieldName): Option[String] = Some(fn.lowerName)
}
