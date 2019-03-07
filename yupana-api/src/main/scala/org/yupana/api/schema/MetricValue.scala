package org.yupana.api.schema

trait MetricValue {
  val metric: Metric
  val value: metric.T

  override def toString: String = s"MetricValue(${metric.name}, $value)"
}

object MetricValue {
  def apply[T](fld: Metric.Aux[T], v: T): MetricValue = new MetricValue {
    val metric: Metric.Aux[T] = fld
    val value: T = v
  }
}
