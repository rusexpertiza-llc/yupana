package org.yupana.core.utils.metric

trait MetricQueryCollector extends Serializable {
  def uuid: String

  def dynamicMetric(name: String): Metric

  def finish(): Unit

  def isEnabled: Boolean

  val createQueries: Metric = NoMetric
  val createDimensionFilters: Metric = NoMetric
  val createScans: Metric = NoMetric
  val loadTags: Metric = NoMetric
  val filterRows: Metric = NoMetric
  val windowFunctionsCheck: Metric = NoMetric
  val windowFunctions: Metric = NoMetric
  val mapOperation: Metric = NoMetric
  val postMapOperation: Metric = NoMetric
  val reduceOperation: Metric = NoMetric
  val postFilter: Metric = NoMetric
  val collectResultRows: Metric = NoMetric
  val extractDataTags: Metric = NoMetric
  val extractDataComputation: Metric = NoMetric
  val getResult: Metric = NoMetric
  val parseResult: Metric = NoMetric
}

object NoMetricCollector extends MetricQueryCollector {

  override def dynamicMetric(name: String): Metric = NoMetric

  override def finish(): Unit = {}

  override val uuid: String = ""

  override val isEnabled: Boolean = false
}

trait Metric extends Serializable {
  def measure[T](f: => T): T
}

object NoMetric extends Metric {
  override def measure[T](f: => T): T = f
}


