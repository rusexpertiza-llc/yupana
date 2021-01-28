package org.yupana.core

import org.joda.time.LocalDateTime
import org.yupana.core.dao.InvalidPeriodsDao
import org.yupana.core.model.InvalidPeriod

class QueryEngine(val invalidPeriodsDao: InvalidPeriodsDao) {
  def execute[T](query: AuxQuery[T]): T = {
    query.execute(this)
  }
}

trait AuxQuery[T] {
  def execute(engine: QueryEngine): T
}

case class GetInvalidPeriodsQuery(rollupDateFrom: LocalDateTime, rollupDateTo: LocalDateTime)
    extends AuxQuery[Iterable[InvalidPeriod]] {
  override def execute(engine: QueryEngine): Iterable[InvalidPeriod] = {
    engine.invalidPeriodsDao.getInvalidPeriods(rollupDateFrom, rollupDateTo)
  }
}
