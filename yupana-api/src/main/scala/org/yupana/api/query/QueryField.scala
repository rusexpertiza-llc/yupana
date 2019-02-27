package org.yupana.api.query

case class QueryField(name: String, expr: Expression) {
  override def toString: String = s"$expr as $name"
}
