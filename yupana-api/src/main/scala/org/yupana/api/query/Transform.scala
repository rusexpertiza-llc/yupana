package org.yupana.api.query

`import org.yupana.api.query.Expression.Condition

trait Transform

case class Replace(from: Set[Condition], to: Condition) extends Transform
case class Original(conditions: Set[Condition]) extends Transform
