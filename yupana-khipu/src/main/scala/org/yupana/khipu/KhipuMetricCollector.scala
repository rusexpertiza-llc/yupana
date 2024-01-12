package org.yupana.khipu

import com.typesafe.scalalogging.StrictLogging
import org.yupana.metrics.{ Metric, MetricCollector, MetricImpl }

object KhipuMetricCollector extends MetricCollector with StrictLogging {

  private def createMetric(name: String): MetricImpl = new MetricImpl(name, this)

  object Metrics {
    val allocateBlock: MetricImpl = createMetric("allocateBlock")
    val findFreeBlock: MetricImpl = createMetric("findFreeBlocks")
    val put: MetricImpl = createMetric("put")
    val merge: MetricImpl = createMetric("merge")
    val splitAndWriteLeafBlocks: MetricImpl = createMetric("splitAndWriteLeafBlocks")
    val splitAndWriteNodeBlocks: MetricImpl = createMetric("splitAndWriteNodeBlocks")
    val writeLeafBlock: MetricImpl = createMetric("writeLeafBlock")
    val writeNodeBlock: MetricImpl = createMetric("writeNodeBlock")
    val load: MetricImpl = createMetric("load")
  }

  import Metrics._

  override def allMetrics: Seq[Metric] = Seq(
    allocateBlock,
    findFreeBlock,
    put,
    splitAndWriteNodeBlocks,
    splitAndWriteLeafBlocks,
    writeNodeBlock,
    writeLeafBlock,
    merge,
    load
  )

  override def id: String = "KhipuStorage"

  override def operationName: String = "none"

  override def partitionId: Option[String] = None

  override def dynamicMetric(name: String): Metric =
    throw new UnsupportedOperationException("Dynamic metrics are not supported")

  override def isEnabled: Boolean = true
  override def checkpoint(): Unit = {}

  override def metricUpdated(metric: Metric, time: Long): Unit = {}

  def reset(): Unit = {
    allMetrics.foreach(_.reset())
  }

  def logStat(showNulls: Boolean = true): Unit = {

    println("Khipu performance statistics")
    allMetrics.foreach { metric =>
      val time = metric.time
      val count = metric.count
      if (showNulls || time > 0 || count > 0)
        println(s" ==> stage: ${metric.name}; time: ${formatNanoTime(time)}; count: $count")
    }
  }

  private def formatNanoTime(value: Long): String = {
    new java.text.DecimalFormat("#.##########").format(value / 1000000000.0)
  }
}
