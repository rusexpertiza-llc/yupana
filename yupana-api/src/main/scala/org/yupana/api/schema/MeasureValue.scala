package org.yupana.api.schema

trait MeasureValue {
  val measure: Measure
  val value: measure.T

  override def toString: String = s"MeasureValue(${measure.name}, $value)"
}

object MeasureValue {
  def apply[T](fld: Measure.Aux[T], v: T): MeasureValue = new MeasureValue {
    val measure: Measure.Aux[T] = fld
    val value: T = v
  }
}
