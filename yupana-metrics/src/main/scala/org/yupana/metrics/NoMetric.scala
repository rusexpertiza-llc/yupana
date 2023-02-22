package org.yupana.metrics

object NoMetric extends Metric {
  override val name: String = "NONE"

  override val time: Long = 0L
  override val count: Long = 0L

  override def reset(): Unit = {}

  @inline
  override def measure[T](count: Int)(f: => T): T = f
}
