package org.yupana.api.query

/**
  * Query field is an expression with name
  * @param name this field name
  * @param expr expression
  */
case class QueryField(name: String, expr: Expression) {
  override def toString: String = s"$expr as $name"
}
