package org.yupana.khipu

import org.yupana.api.query.Expression
import org.yupana.api.schema.{ Dimension, Table }
import org.yupana.core.model.InternalQuery
import org.yupana.core.utils.metric.MetricQueryCollector

import scala.collection.mutable

case class InternalQueryContext(
    table: Table,
    exprsIndexSeq: Seq[(Expression[_], Int)],
    dimIndexMap: mutable.Map[Dimension, Int],
    metricsCollector: MetricQueryCollector
)

object InternalQueryContext {
  def apply(query: InternalQuery, metricCollector: MetricQueryCollector): InternalQueryContext = {
    val dimIndexMap = mutable.HashMap(query.table.dimensionSeq.zipWithIndex: _*)

    val exprsIndexSeq = query.exprs.toSeq.zipWithIndex

    new InternalQueryContext(query.table, exprsIndexSeq, dimIndexMap, metricCollector)
  }
}
