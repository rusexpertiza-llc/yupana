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

import org.yupana.core.model.{ BatchDataset, HashTableDataset }
import org.yupana.core.utils.CollectionUtils

import scala.reflect.ClassTag

class IteratorMapReducible(reduceLimit: Int = Int.MaxValue) extends MapReducible[Iterator] {

  override def empty[A: ClassTag]: Iterator[A] = Iterator.empty
  override def singleton[A: ClassTag](a: A): Iterator[A] = Iterator(a)
  override def filter[A: ClassTag](it: Iterator[A])(f: A => Boolean): Iterator[A] = it.filter(f)

  override def map[A: ClassTag, B: ClassTag](it: Iterator[A])(f: A => B): Iterator[B] = it.map(f)
  override def flatMap[A: ClassTag, B: ClassTag](it: Iterator[A])(f: A => Iterable[B]): Iterator[B] = it.flatMap(f)

  override def batchFlatMap[A, B: ClassTag](it: Iterator[A], size: Int)(
      f: Seq[A] => IterableOnce[B]
  ): Iterator[B] = {
    it.grouped(size).flatMap(f)
  }

  override def fold[A: ClassTag](it: Iterator[A])(zero: A)(f: (A, A) => A): A = it.fold(zero)(f)

  override def reduce[A: ClassTag](it: Iterator[A])(f: (A, A) => A): A = it.reduce(f)

  override def reduceByKey[K: ClassTag, V: ClassTag](it: Iterator[(K, V)])(f: (V, V) => V): Iterator[(K, V)] = {
    CollectionUtils.reduceByKey(it, reduceLimit)(f)
  }

  override def aggregateByKey[K: ClassTag, A: ClassTag, B: ClassTag](
      it: Iterator[(K, A)]
  )(createZero: A => B, seqOp: (B, A) => B, combOp: (B, B) => B): Iterator[(K, B)] =
    CollectionUtils.foldByKey(it)(createZero, seqOp)

  override def aggregate[A: ClassTag, B: ClassTag](
      it: Iterator[A]
  )(createZero: A => B, seqOp: (B, A) => B, combOp: (B, B) => B): Iterator[B] = {
    if (it.hasNext) {
      val first = it.next()
      val r = it.foldLeft(createZero(first))(seqOp)
      Iterator(r)
    } else {
      Iterator.empty[B]
    }
  }

  override def aggregate2(it: Iterator[BatchDataset], queryContext: QueryContext)(
      foldOp: (HashTableDataset, BatchDataset) => Unit,
      combOp: (HashTableDataset, BatchDataset) => Unit
  ): Iterator[BatchDataset] = {
    val acc = HashTableDataset(queryContext)
    it.foreach { batch =>
      foldOp(acc, batch)
    }
    acc.iterator
  }

  override def distinct[A: ClassTag](it: Iterator[A]): Iterator[A] = it.toSet.iterator

  override def limit[A: ClassTag](it: Iterator[A])(n: Int): Iterator[A] = it.take(n)

  override def materialize[A: ClassTag](it: Iterator[A]): Seq[A] = it.toList

  override def concat[A: ClassTag](a: Iterator[A], b: Iterator[A]): Iterator[A] = a ++ b

}

object IteratorMapReducible {
  val iteratorMR: MapReducible[Iterator] = new IteratorMapReducible()
}
