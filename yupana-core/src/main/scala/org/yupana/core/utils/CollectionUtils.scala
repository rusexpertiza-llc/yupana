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

import java.util
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

object CollectionUtils {

  def reduceByKey[K, A](it: Iterator[(K, A)], limit: Int = Int.MaxValue)(func: (A, A) => A): Iterator[(K, A)] = {
    val map = new util.HashMap[K, A]()
    it.foreach {
      case (k, v) =>
        val old = map.get(k)
        val n = if (old != null) func(old, v) else v
        map.put(k, n)
        if (limit < map.size()) {
          throw new IllegalStateException(s"reduceByKey operation is out of limit = $limit")
        }
    }
    map.asScala.iterator
  }

  def foldByKey[K, A, B](it: Iterator[(K, A)])(z: A => B, func: (B, A) => B): Iterator[(K, B)] = {
    val map = new util.HashMap[K, B]()
    it.foreach {
      case (k, v) =>
        val old = map.get(k)
        val n = if (old != null) func(old, v) else z(v)
        map.put(k, n)
    }
    map.asScala.iterator
  }

  def crossJoin[T](list: List[List[T]]): List[List[T]] = {

    @tailrec
    def crossJoin(acc: List[List[T]], rest: List[List[T]]): List[List[T]] = {
      rest match {
        case Nil => acc
        case x :: xs =>
          val next = for {
            i <- x
            j <- acc
          } yield i :: j
          crossJoin(next, xs)
      }
    }

    crossJoin(List(Nil), list.reverse)
  }

  def alignByKey[K, V](keyOrder: Seq[K], values: Seq[V], getKey: V => K): Seq[V] = {
    val orderMap = keyOrder.zipWithIndex.toMap
    values.sortBy { v =>
      val k = getKey(v)
      orderMap.getOrElse(k, throw new IllegalArgumentException(s"Unknown key $k for value $v"))
    }
  }

  def intersectAll[T](sets: Seq[Set[T]]): Set[T] = {
    if (sets.nonEmpty) sets.reduce(_ intersect _) else Set.empty
  }

  def mergeMaps[K, V, U](a: Map[K, V], b: Map[K, V], map: V => U, reduce: (U, U) => U): Map[K, U] = {
    (a.keySet ++ b.keySet).map { k =>
      val v = (a.get(k), b.get(k)) match {
        case (Some(x), Some(y)) => reduce(map(x), map(y))
        case (Some(x), None)    => map(x)
        case (None, Some(y))    => map(y)
        case _                  => throw new IllegalStateException("This should never happen")
      }
      k -> v
    }.toMap
  }

  def mergeMaps[K, V](a: Map[K, V], b: Map[K, V], reduce: (V, V) => V): Map[K, V] =
    mergeMaps[K, V, V](a, b, identity, reduce)
}
