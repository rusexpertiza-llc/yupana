package org.yupana.api.types

case class Comparison[T](function: (T, T) => Boolean, name: String) extends Serializable {
  def apply(x: T, y: T): Boolean = function(x, y)
  override def toString: String = name
}

object Comparison {
  val EQ = "EQ"
  val NE = "NE"
  val GT = "GT"
  val LT = "LT"
  val GE = "GE"
  val LE = "LE"

  def eq[T](implicit ord: Ordering[T]): Comparison[T] = Comparison(ord.equiv, "==")
  def ne[T](implicit ord: Ordering[T]): Comparison[T] = Comparison((x, y) => !ord.equiv(x, y), "!=")
  def lt[T](implicit ord: Ordering[T]): Comparison[T] = Comparison(ord.lt, "<")
  def gt[T](implicit ord: Ordering[T]): Comparison[T] = Comparison(ord.gt, ">")
  def le[T](implicit ord: Ordering[T]): Comparison[T] = Comparison(ord.lteq, "<=")
  def ge[T](implicit ord: Ordering[T]): Comparison[T] = Comparison(ord.gteq, ">=")

  def ordComparisons[T : Ordering]: Map[String, Comparison[T]] = Map(
    EQ -> Comparison.eq[T],
    NE -> Comparison.ne[T],
    GT -> Comparison.gt[T],
    LT -> Comparison.lt[T],
    GE -> Comparison.ge[T],
    LE -> Comparison.le[T]
  )

  def tupleComparisons[T, U](tc: Map[String, Comparison[T]], uc: Map[String, Comparison[U]]): Map[String, Comparison[(T, U)]] = {
    val commonCmps = tc.keySet intersect uc.keySet
    commonCmps.map(c => c -> Comparison[(T, U)]((a, b) => tc(c)(a._1, b._1) && uc(c)(a._2, b._2), c)).toMap
  }
}
