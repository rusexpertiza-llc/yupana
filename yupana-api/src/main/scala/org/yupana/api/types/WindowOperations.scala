package org.yupana.api.types

import scala.annotation.implicitNotFound

/**
  * Window function definition
  * @tparam T input type
  */
trait WindowOperation[T] extends Serializable {
  /** result type */
  type Out
  /** This operation name */
  val name: String

  /**
    * Applies window operation
    * @param values array of optional input values
    * @param index current index
    * @param wo instance of window operations implementation
    * @return result value
    */
  def apply(values: Array[Option[T]], index: Int)(implicit wo: WindowOperations): Option[Out]
  val dataType: DataType.Aux[Out]
}

object WindowOperation {
  type Aux[T, V] = WindowOperation[T] { type Out = V }

  val LAG = "lag"
  val functions = Set(LAG)

  def lag[T](implicit dt: DataType.Aux[T]): WindowOperation.Aux[T, T] = create(LAG, _.lag, dt)

  def create[T, V](n: String, f: WindowOperations => (Array[Option[T]], Int) => Option[V], dt: DataType.Aux[V]): Aux[T, V] = new WindowOperation[T] {
    override type Out = V
    override val name: String = n
    override def apply(values: Array[Option[T]], index: Int)(implicit wo: WindowOperations): Option[V] = f(wo)(values, index)
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

trait WindowOperations {
  def lag[T](values: Array[Option[T]], index: Int): Option[T]
}
