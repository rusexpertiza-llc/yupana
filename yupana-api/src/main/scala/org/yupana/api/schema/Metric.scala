package org.yupana.api.schema

import org.yupana.api.types.DataType

/**
  * Defines metrics in some table.
  */
trait Metric extends Serializable {
  /** Type of a value for this metric */
  type T

  /** Metric name */
  val name: String

  /** Tag is a primary identifier of the metric and must be unique in one table */
  val tag: Byte

  /** Value data type */
  val dataType: DataType.Aux[T]

  /**
    * Group for this metric. Might be used to define different storages for different metrics in the low level database,
    * for example column families in HBase.
    */
  val group: Int

  override def toString: String = s"Metric($name)"
}

object Metric {
  type Aux[T0] = Metric { type T = T0 }

  val defaultGroup: Int = 1

  def apply[T0](name: String, tag: Byte, group: Int = defaultGroup)(implicit dt: DataType.Aux[T0]): Aux[T0] = {
    val (n, t, g) = (name, tag, group)

    new Metric {
      override type T = T0
      override val name: String = n
      override val tag: Byte = t
      override val dataType: DataType.Aux[T0] = dt
      override val group: Int = g
    }
  }
}
