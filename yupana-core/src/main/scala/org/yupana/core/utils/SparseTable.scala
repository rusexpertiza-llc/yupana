/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.core.utils

class SparseTable[R, C, +V](val values: Map[R, Map[C, V]]) extends Table[R, C, V] with TableOps[R, C, V, SparseTable] {

  override def hashCode(): Int = values.hashCode()

  override def equals(o: Any): Boolean = {
    o match {
      case that: SparseTable[_, _, _] => this.values == that.values
      case _                          => false
    }
  }

  override def toString: String = {
    values
      .map {
        case (k, v) =>
          val vs = v.mkString("(", ", ", ")")
          s"$k => $vs"
      }
      .mkString("SparseTable(", ", ", ")")
  }

  override def tableFactory: TableFactory[SparseTable] = SparseTable

  override def get(row: R, column: C): Option[V] = values.get(row).flatMap(_.get(column))
  override def row(r: R): Map[C, V] = values.getOrElse(r, Map.empty)
  override def column(c: C): Map[R, V] = values.flatMap {
    case (rowKey, rowValues) => rowValues.get(c).map(rowKey -> _)
  }

  override def transpose[V1 >: V]: SparseTable[C, R, V1] = {
    val transposed = values.foldLeft(Map.empty[C, Map[R, V]]) {
      case (a, (r, cv)) =>
        cv.foldLeft(a) {
          case (aa, (c, v)) =>
            val old = aa.getOrElse(c, Map.empty)
            aa + (c -> (old + (r -> v)))
        }
    }

    new SparseTable[C, R, V](transposed)
  }

  override def mapRowKeys[RR, V1 >: V](f: R => RR): SparseTable[RR, C, V1] = {
    new SparseTable(values.map { case (k, v) => f(k) -> v })
  }

  override def toIterator: Iterator[(R, C, V)] = {
    values.toIterator.flatMap { case (r, cv) => cv.toIterator.map { case (c, v) => (r, c, v) } }
  }

  override def +[V1 >: V](e: (R, C, V1)): SparseTable[R, C, V1] = {
    val col = values.getOrElse(e._1, Map.empty)
    val newValues = values + (e._1 -> (col + (e._2 -> e._3)))
    new SparseTable(newValues)
  }

  override def ++[V2 >: V](t: Table[R, C, V2]): SparseTable[R, C, V2] = {
    val v2: SparseTable[R, C, V2] = this

    t match {
      case that: SparseTable[R, C, V2] =>
        val newValues = that.values.foldLeft(v2.values) {
          case (a, (r, cv)) =>
            val col = a.getOrElse(r, Map.empty)
            a + (r -> (col ++ cv))
        }

        new SparseTable(newValues)
      case _ =>
        t.toIterator.foldLeft(v2) { case (a, v) => a + v }
    }
  }
}

object SparseTable extends TableFactory[SparseTable] {
  override def empty[R, C, V]: SparseTable[R, C, V] = new SparseTable[R, C, V](Map.empty)

  override def apply[R, C, V](items: TraversableOnce[(R, C, V)]): SparseTable[R, C, V] = {
    val allValues = items.aggregate(Map.empty[R, Map[C, V]])(
      {
        case (m, (r, c, v)) =>
          val col = m.getOrElse(r, Map.empty)
          m + (r -> (col + (c -> v)))
      }, {
        case (a, b) =>
          val ks = a.keySet ++ b.keySet
          ks.map { k => k -> (a.getOrElse(k, Map.empty) ++ b.getOrElse(k, Map.empty)) }.toMap
      }
    )

    new SparseTable(allValues)
  }

  def apply[R, C, V](values: Map[R, Map[C, V]]) = new SparseTable[R, C, V](values)

  def apply[R, C, V](values: (R, Map[C, V])*) = new SparseTable[R, C, V](values.toMap)
}
