package org.yupana.api.utils

trait DimOrdering[T] {
  def gt(a: T, b: T): Boolean
  def lt(a: T, b: T): Boolean

  def gte(a: T, b: T): Boolean = !lt(a, b)
  def lte(a: T, b: T): Boolean = !lt(b, a)

  def min(a: T, b: T): T
  def max(a: T, b: T): T
}

object DimOrdering {

  implicit val intDimOrdering: DimOrdering[Int] = fromCmp(java.lang.Integer.compareUnsigned)
  implicit val longDimOrdering: DimOrdering[Long] = fromCmp(java.lang.Long.compareUnsigned)
  implicit val stringDimOrdering: DimOrdering[String] = fromCmp(Ordering[String].compare)

  def fromCmp[T](cmp: (T, T) => Int): DimOrdering[T] = new DimOrdering[T] {
    override def lt(x: T, y: T): Boolean = cmp(x, y) < 0
    override def gt(x: T, y: T): Boolean = cmp(x, y) > 0

    override def max(a: T, b: T): T = if (gte(a, b)) a else b
    override def min(a: T, b: T): T = if (lte(a, b)) a else b
  }
}
