package org.yupana.core.model

import org.joda.time.DateTime

case class InvalidPeriod(rollupTime: DateTime, from: DateTime, to: DateTime)

object InvalidPeriod {
  val rollupTimeColumn = "rollup_time"
  val fromColumn = "from"
  val toColumn = "to"
}
