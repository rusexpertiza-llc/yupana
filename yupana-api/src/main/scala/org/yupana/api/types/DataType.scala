package org.yupana.api.types

import org.joda.time.Period
import org.yupana.api.Time

trait DataType extends Serializable {
  type T
  val meta: DataTypeMeta[T]
  val readable: Readable[T]
  val writable: Writable[T]
  def operations: TypeOperations[T]

  override def equals(obj: scala.Any): Boolean = {
    if (obj == null) false
    else obj match {
      case that: DataType =>
        this.meta == that.meta
      case _ => false
    }
  }

  override def toString: String = s"ResultType(${meta.sqlTypeName})"
}

object DataType {
  private lazy val types = Seq(
    DataType[String],
    DataType[Double],
    DataType[Long],
    DataType[Int],
    DataType[BigDecimal],
    DataType[Time]
  ).map(t => t.meta.sqlTypeName -> t).toMap

  def bySqlName(sqlName: String): DataType = types(sqlName)

  type Aux[TT] = DataType { type T = TT }

  def apply[T](implicit rt: DataType.Aux[T]): DataType.Aux[T] = rt

  implicit val stringRt: DataType.Aux[String] = DataType[String](r => TypeOperations.stringOperations(r))

  implicit val timeRt: DataType.Aux[Time] = DataType[Time](r => TypeOperations.timeOperations(r))

  implicit val periodRt: DataType.Aux[Period] = DataType[Period](r => TypeOperations.periodOperations(r))

  implicit def intRt[T: Readable : Writable : DataTypeMeta : Integral]: DataType.Aux[T] =
    DataType[T]((r: DataType.Aux[T]) => TypeOperations.intOperations(r))

  implicit def fracRt[T: Readable : Writable : DataTypeMeta : Fractional]: DataType.Aux[T] =
    DataType[T]((r: DataType.Aux[T]) => TypeOperations.fracOperations(r))

  implicit def tupleRt[TT, UU](implicit rtt: DataType.Aux[TT], rtu: DataType.Aux[UU]): DataType.Aux[(TT, UU)] = new DataType {
    override type T = (TT, UU)
    override val meta: DataTypeMeta[T] = DataTypeMeta.tuple(rtt.meta, rtu.meta)
    override val readable: Readable[T] = Readable.noop
    override val writable: Writable[T] = Writable.noop

    override def operations: TypeOperations[T] = TypeOperations.tupleOperations(rtt, rtu)
  }

  private def apply[TT](getOps: DataType.Aux[TT] => TypeOperations[TT])(implicit
                                                                        r: Readable[TT],
                                                                        w: Writable[TT],
                                                                        m: DataTypeMeta[TT]
  ): DataType.Aux[TT] = new DataType {
    override type T = TT
    override val meta: DataTypeMeta[T] = m
    override val readable: Readable[T] = r
    override val writable: Writable[T] = w
    override lazy val operations: TypeOperations[TT] = getOps(this)
  }
}
