package org.yupana.api.query

import org.yupana.api.query.Expression.Condition

trait Transform

case class Replace(in: Set[Condition], out: Condition) extends Transform
case class Original(in: Set[Condition]) extends Transform
