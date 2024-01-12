package org.yupana.khipu.examples

import org.yupana.core.dao.{ QueryMetricsFilter, TsdbQueryMetricsDao }
import org.yupana.core.model.TsdbQueryMetrics
import org.yupana.core.utils.metric.InternalMetricData

class DummyMetricsDao extends TsdbQueryMetricsDao {
  override def queriesByFilter(filter: Option[QueryMetricsFilter], limit: Option[Int]): Iterator[TsdbQueryMetrics] =
    Iterator.empty

  override def saveQueryMetrics(metrics: List[InternalMetricData]): Unit = ()

  override def deleteMetrics(filter: QueryMetricsFilter): Int = 0
}
