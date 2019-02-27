package org.yupana.api.types

import java.sql.Types

import org.joda.time.Period
import org.yupana.api.Time

case class DataTypeMeta[T](
  sqlType: Int,
  displaySize: Int,
  sqlTypeName: String,
  javaTypeName: String,
  precision: Int,
  isSigned: Boolean,
  scale: Int
)

object DataTypeMeta {
  private val SIGNED_TYPES = Set(Types.INTEGER, Types.BIGINT, Types.DOUBLE, Types.DECIMAL)

  implicit val stringMeta: DataTypeMeta[String] = DataTypeMeta(Types.VARCHAR, Integer.MAX_VALUE, "VARCHAR", classOf[java.lang.String], Integer.MAX_VALUE, 0)
  implicit val intMeta: DataTypeMeta[Int] = DataTypeMeta(Types.INTEGER, 10, "INTEGER", classOf[java.lang.Integer], 10, 0)
  implicit val doubleMeta: DataTypeMeta[Double] = DataTypeMeta(Types.DOUBLE, 25, "DOUBLE", classOf[java.lang.Double], 17, 17)
  implicit val longMeta: DataTypeMeta[Long] = DataTypeMeta(Types.BIGINT, 20, "BIGINT", classOf[java.lang.Long], 19, 0)
  implicit val decimalMeta: DataTypeMeta[BigDecimal] = DataTypeMeta(Types.DECIMAL, 131089, "DECIMAL", classOf[java.math.BigDecimal], 0, 0)
  implicit val timestampMeta: DataTypeMeta[Time] = DataTypeMeta(Types.TIMESTAMP, 23, "TIMESTAMP", classOf[java.sql.Timestamp], 23, 6)
  implicit val periodMeta: DataTypeMeta[Period] = DataTypeMeta(Types.VARCHAR, 20, "PERIOD", classOf[java.lang.String], 20, 0)

  implicit def optionMeta[T](implicit meta: DataTypeMeta[T]): DataTypeMeta[Option[T]] =
    DataTypeMeta(meta.sqlType, meta.displaySize, meta.sqlTypeName, meta.javaTypeName, meta.precision, meta.isSigned, meta.scale)

  def apply[T](t: Int, ds: Int, tn: String, jt: Class[_], p: Int, s: Int): DataTypeMeta[T] =
    DataTypeMeta(t, ds, tn, jt.getCanonicalName, p, SIGNED_TYPES.contains(t), s)

  def tuple[T, U](implicit tMeta: DataTypeMeta[T], uMeta: DataTypeMeta[U]): DataTypeMeta[(T, U)] = DataTypeMeta(
    Types.OTHER,
    tMeta.displaySize + uMeta.displaySize,
    s"${tMeta.sqlTypeName}_${uMeta.sqlTypeName}",
    classOf[(T, U)].getCanonicalName,
    tMeta.precision + uMeta.precision,
    isSigned = false,
    0
  )
}
