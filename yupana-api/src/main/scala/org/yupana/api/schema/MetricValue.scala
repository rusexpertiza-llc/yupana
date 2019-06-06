package org.yupana.api.schema

/**
  * Metric and it's value
  */
trait MetricValue {
  val metric: Metric
  val value: metric.T

  override def toString: String = s"MetricValue(${metric.name}, $value)"
}

object MetricValue {
  def apply[T](m: Metric.Aux[T], v: T): MetricValue = new MetricValue {
    val metric: Metric.Aux[T] = m
    val value: T = v
  }
}
