package org.yupana.api.schema

import org.yupana.api.types.DataType

trait Measure extends Serializable {
  type T
  val name: String
  val tag: Byte
  val dataType: DataType.Aux[T]
  val group: Int

  override def toString: String = s"Measure($name)"
}

object Measure {
  type Aux[T0] = Measure { type T = T0 }

  val defaultGroup: Int = 1

  def apply[T0](name: String, tag: Byte, group: Int = defaultGroup)(implicit dt: DataType.Aux[T0]): Aux[T0] = {
    val (n, t, g) = (name, tag, group)

    new Measure {
      override type T = T0
      override val name: String = n
      override val tag: Byte = t
      override val dataType: DataType.Aux[T0] = dt
      override val group: Int = g
    }
  }
}
