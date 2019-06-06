package org.yupana.core.model

import org.yupana.api.query.{Condition, Expression}
import org.yupana.api.schema.Table

case class InternalQuery(table: Table, exprs: Set[Expression], condition: Condition)
