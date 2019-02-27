package org.yupana.api.types

import scala.annotation.implicitNotFound

trait WindowOperation[T] extends Serializable {
  type Out
  val name: String
  val func: (Array[Option[T]], Int) => Option[Out]
  val dataType: DataType.Aux[Out]
}

object WindowOperation {
  type Aux[T, V] = WindowOperation[T] { type Out = V }

  val LAG = "lag"
  val functions = Set(LAG)

  def lag[T](implicit dt: DataType.Aux[T]): WindowOperation.Aux[T, T] =
    WindowOperation[T, T](LAG, (g, i) => if (i > 0) g(i - 1) else None, dt)

  def apply[T, V](n: String, f: (Array[Option[T]], Int) => Option[V], dt: DataType.Aux[V]): Aux[T, V] = new WindowOperation[T] {
    override type Out = V
    override val name: String = n
    override val func: (Array[Option[T]], Int) => Option[V] = f
    override val dataType: DataType.Aux[V] = dt
  }

}

@implicitNotFound("Type window operations for type ${T} not found")
case class TypeWindowOperations[T](operations: Map[String, WindowOperation[T]]) {
  def apply(name: String): Option[WindowOperation[T]] = operations.get(name)
}

object TypeWindowOperations {
  import WindowOperation._

  def getFunction(name:String, dataType: DataType): Option[WindowOperation.Aux[dataType.T, dataType.T]] = {
    name match {
      case LAG => Some(WindowOperation.lag(dataType))
      case _ => None
    }
  }

  def apply[T: DataType.Aux](ops: (String, WindowOperation[T])*): TypeWindowOperations[T] = TypeWindowOperations[T](Map(ops:_*))

  implicit def typeIndependence[T : DataType.Aux]: TypeWindowOperations[T] = TypeWindowOperations(
    LAG -> WindowOperation.lag
  )

}
