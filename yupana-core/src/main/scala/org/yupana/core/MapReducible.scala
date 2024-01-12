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

import scala.reflect.ClassTag

/**
  * Defines basic operations on `Collection`
  * @tparam Collection collection for which operations are defined
  */
trait MapReducible[Collection[_]] extends Serializable {
  def empty[A: ClassTag]: Collection[A]

  def singleton[A: ClassTag](a: A): Collection[A]
  def filter[A: ClassTag](c: Collection[A])(f: A => Boolean): Collection[A]

  def aggregateByKey[K: ClassTag, A: ClassTag, B: ClassTag](
      c: Collection[(K, A)]
  )(createZero: A => B, seqOp: (B, A) => B, combOp: (B, B) => B): Collection[(K, B)]

  def aggregate[A: ClassTag, B: ClassTag](
      c: Collection[A]
  )(createZero: A => B, seqOp: (B, A) => B, combOp: (B, B) => B): Collection[B]

  def map[A: ClassTag, B: ClassTag](c: Collection[A])(f: A => B): Collection[B]
  def flatMap[A: ClassTag, B: ClassTag](mr: Collection[A])(f: A => Iterable[B]): Collection[B]

  def batchFlatMap[A, B: ClassTag](c: Collection[A], size: Int)(f: Seq[A] => IterableOnce[B]): Collection[B]

  def fold[A: ClassTag](c: Collection[A])(zero: A)(f: (A, A) => A): A
  def reduce[A: ClassTag](c: Collection[A])(f: (A, A) => A): A
  def reduceByKey[K: ClassTag, V: ClassTag](c: Collection[(K, V)])(f: (V, V) => V): Collection[(K, V)]

  def distinct[A: ClassTag](c: Collection[A]): Collection[A]

  def limit[A: ClassTag](c: Collection[A])(n: Int): Collection[A]

  def concat[A: ClassTag](a: Collection[A], b: Collection[A]): Collection[A]

  def materialize[A: ClassTag](c: Collection[A]): Seq[A]
}
