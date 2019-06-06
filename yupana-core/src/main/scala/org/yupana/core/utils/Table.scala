package org.yupana.core.utils

import scala.language.higherKinds

trait Table[R, C, +V] extends TableOps[R, C, V, Table]

trait TableOps[R, C, +V, +TT[_, _, _]] {

  def tableFactory: TableFactory[TT]

  def get(row: R, column: C): Option[V]

  def row(r: R): Map[C, V]
  def column(c: C): Map[R, V]

  def transpose[V1 >: V]: TT[C, R, V1]

  def mapRowKeys[RR, V1 >: V](f: R => RR): TT[RR, C, V1]

  def toIterator: Iterator[(R, C, V)]

  def map[R2, C2, V2](f: ((R, C, V)) => (R2, C2, V2)): TT[R2, C2, V2] = {
    tableFactory(toIterator.map(f))
  }

  def flatMap[R2, C2, V2](f: ((R, C, V)) => Iterable[(R2, C2, V2)]): TT[R2, C2, V2] = {
    tableFactory(toIterator.flatMap(f))
  }

  def +[V1 >: V](e: (R, C, V1)): TT[R, C, V1]

  def ++[V2 >: V](t: Table[R, C, V2]): TT[R, C, V2]
}

abstract class TableFactory[+TT[_, _, _]]  {
  def empty[R, C, V]: TT[R, C, V]
  def apply[R, C, V](items: TraversableOnce[(R, C, V)]): TT[R, C, V]
}
