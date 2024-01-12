package org.yupana.khipu.examples

import org.yupana.core.dao.ChangelogDao
import org.yupana.core.model.UpdateInterval

class DummyChangelogDao extends ChangelogDao {
  override def putUpdatesIntervals(intervals: Seq[UpdateInterval]): Unit = ()

  override def getUpdatesIntervals(
      tableName: Option[String],
      updatedAfter: Option[Long],
      updatedBefore: Option[Long],
      recalculatedAfter: Option[Long],
      recalculatedBefore: Option[Long],
      updatedBy: Option[String]
  ): Iterable[UpdateInterval] = Seq.empty
}
