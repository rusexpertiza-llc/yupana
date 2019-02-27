package org.yupana.api.types

class TypeConverter[In, Out](
  val dataType: DataType.Aux[Out],
  val functionName: String,
  val direct: In => Out,
  val reverse: Out => Option[In]
)

object TypeConverter {

  def apply[T, U](implicit a: DataType.Aux[T], b: DataType.Aux[U]): Option[TypeConverter[T, U]] = {
    converters.get((a.meta.sqlTypeName, b.meta.sqlTypeName)).asInstanceOf[Option[TypeConverter[T, U]]]
  }

  val double2BigDecimal: TypeConverter[Double, BigDecimal] = of(x => BigDecimal(x), x => Some(x.toDouble))
  val long2BigDecimal: TypeConverter[Long, BigDecimal] = of(x => BigDecimal(x), x => if (x.isValidLong) Some(x.longValue) else None)
  val long2Double: TypeConverter[Long, Double] = of(_.toDouble, _ => None)
  val int2Long: TypeConverter[Int, Long] = of(_.toLong, x => if (x.isValidInt) Some(x.toInt) else None)
  val int2BigDecimal: TypeConverter[Int, BigDecimal] = of(x => BigDecimal(x), x => if (x.isValidInt) Some(x.toInt) else None)

  def of[T, U](f: T => U, rev: U => Option[T])(implicit
    rtt: DataType.Aux[T],
    rtu: DataType.Aux[U]
  ): TypeConverter[T, U] = {
    new TypeConverter[T, U](
      rtu, rtt.meta.sqlTypeName.toLowerCase + "2" + rtu.meta.sqlTypeName.toLowerCase, f, rev
    )
  }

  private def entry[T, U](tc: TypeConverter[T, U])(implicit
    dtt: DataType.Aux[T],
    dtu: DataType.Aux[U]
  ): ((String, String), TypeConverter[T, U]) = {
    ((dtt.meta.sqlTypeName, dtu.meta.sqlTypeName), tc)
  }

  private val converters: Map[(String, String), TypeConverter[_, _]] = Map(
    entry[Double, BigDecimal](double2BigDecimal),
    entry[Long, BigDecimal](long2BigDecimal),
    entry[Long, Double](long2Double),
    entry[Int, Long](int2Long),
    entry[Int, BigDecimal](int2BigDecimal)
  )

}
