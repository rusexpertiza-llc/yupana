package org.yupana.core.utils.metric

import org.yupana.api.query.Query
import org.yupana.core.model.{ MetricData, QueryStates }

class PersistentMetricQueryCollectorWithExporter(
    collectorContext: QueryCollectorContext,
    query: Query,
    exporterCallBack: => Option[ExporterMetrics => Unit] = None
) extends PersistentMetricQueryCollector(collectorContext, query) {

  override def saveQueryMetrics(state: QueryStates.QueryState): Unit = {
    exporterCallBack.foreach { exporter =>
      val duration = totalDuration
      val labels = Map("id" -> query.id, "state" -> state.name, "is_spark" -> collectorContext.sparkQuery.toString) ++
        getAndResetMetricsData.flatMap { case (pref, mData) => explodeMetricData(pref, mData) }
      exporter(ExporterMetrics(labels, duration))
    }
    super.saveQueryMetrics(state)
  }

  def explodeMetricData(prefix: String, metricData: MetricData): Seq[(String, String)] = {
    Seq(
      s"${prefix}_count" -> metricData.count.toString,
      s"${prefix}_time" -> metricData.time.toString,
      s"${prefix}_speed" -> metricData.speed.toString
    )
  }

}

case class ExporterMetrics(labels: Map[String, String], durationSec: Double)
