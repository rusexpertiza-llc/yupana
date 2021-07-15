package org.yupana.core.utils.metric

import org.yupana.api.query.Query

object NoMetricCollector extends MetricQueryCollector {

  override val createDimensionFilters: Metric = NoMetric
  override val createScans: Metric = NoMetric
  override val scan: Metric = NoMetric
  override val parseScanResult: Metric = NoMetric
  override val dimensionValuesForIds: Metric = NoMetric
  override val readExternalLinks: Metric = NoMetric
  override val extractDataComputation: Metric = NoMetric
  override val filterRows: Metric = NoMetric
  override val windowFunctions: Metric = NoMetric
  override val reduceOperation: Metric = NoMetric
  override val postFilter: Metric = NoMetric
  override val collectResultRows: Metric = NoMetric
  override val dictionaryScan: Metric = NoMetric

  override def dynamicMetric(name: String): Metric = NoMetric

  override def checkpoint(): Unit = {}
  override def metricUpdated(metric: Metric, time: Long): Unit = {}
  override def setRunningPartitions(partitions: Int): Unit = {}
  override def finishPartition(): Unit = {}
  override def finish(): Unit = {}

  override val query: Query = null

  override val isEnabled: Boolean = false
  override def isSparkQuery: Boolean = false

  override val allMetrics: Seq[Metric] = Seq.empty

  override val operationName: String = "UNKNOWN"
  override def startTime: Long = 0L
  override def resultTime: Long = 0L
}

object NoMetric extends Metric {
  override val name: String = "NONE"

  override def time: Long = 0L
  override def count: Long = 0L

  @inline
  override def measure[T](count: Int)(f: => T): T = f
}
