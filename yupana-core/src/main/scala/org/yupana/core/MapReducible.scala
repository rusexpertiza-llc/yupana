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

package org.yupana.core

import org.yupana.core.utils.CollectionUtils

import scala.language.higherKinds
import scala.reflect.ClassTag

/**
  * Defines basic operations on `Collection`
  * @tparam Collection collection for which operations are defined
  */
trait MapReducible[Collection[_]] extends Serializable {
  def singleton[A: ClassTag](a: A): Collection[A]
  def filter[A: ClassTag](c: Collection[A])(f: A => Boolean): Collection[A]

  def map[A: ClassTag, B: ClassTag](c: Collection[A])(f: A => B): Collection[B]
  def flatMap[A: ClassTag, B: ClassTag](mr: Collection[A])(f: A => Iterable[B]): Collection[B]

  def batchFlatMap[A, B: ClassTag](c: Collection[A], size: Int)(f: Seq[A] => Iterator[B]): Collection[B]

  def fold[A: ClassTag](c: Collection[A])(zero: A)(f: (A, A) => A): A
  def reduce[A: ClassTag](c: Collection[A])(f: (A, A) => A): A
  def reduceByKey[K: ClassTag, V: ClassTag](c: Collection[(K, V)])(f: (V, V) => V): Collection[(K, V)]

  def limit[A: ClassTag](c: Collection[A])(n: Int): Collection[A]
}

object MapReducible {
  val iteratorMR: MapReducible[Iterator] = new MapReducible[Iterator] {
    override def singleton[A: ClassTag](a: A): Iterator[A] = Iterator(a)
    override def filter[A: ClassTag](it: Iterator[A])(f: A => Boolean): Iterator[A] = it.filter(f)

    override def map[A: ClassTag, B: ClassTag](it: Iterator[A])(f: A => B): Iterator[B] = it.map(f)
    override def flatMap[A: ClassTag, B: ClassTag](it: Iterator[A])(f: A => Iterable[B]): Iterator[B] = it.flatMap(f)

    override def batchFlatMap[A, B: ClassTag](it: Iterator[A], size: Int)(f: Seq[A] => Iterator[B]): Iterator[B] = {
      it.grouped(size).flatMap(f)
    }

    override def fold[A: ClassTag](it: Iterator[A])(zero: A)(f: (A, A) => A): A = it.fold(zero)(f)

    override def reduce[A: ClassTag](it: Iterator[A])(f: (A, A) => A): A = it.reduce(f)

    override def reduceByKey[K: ClassTag, V: ClassTag](it: Iterator[(K, V)])(f: (V, V) => V): Iterator[(K, V)] = {
      CollectionUtils.reduceByKey(it)(f)
    }

    override def limit[A: ClassTag](it: Iterator[A])(n: Int): Iterator[A] = it.take(n)
  }
}
