package org.yupana.core

import org.yupana.core.utils.CollectionUtils

import scala.reflect.ClassTag

class MapReducibleBase(reduceLimit: Option[Int] = None) extends MapReducible[Iterator] {
  override def singleton[A: ClassTag](a: A): Iterator[A] = Iterator(a)
  override def filter[A: ClassTag](it: Iterator[A])(f: A => Boolean): Iterator[A] = it.filter(f)

  override def map[A: ClassTag, B: ClassTag](it: Iterator[A])(f: A => B): Iterator[B] = it.map(f)
  override def flatMap[A: ClassTag, B: ClassTag](it: Iterator[A])(f: A => Iterable[B]): Iterator[B] = it.flatMap(f)

  override def batchFlatMap[A, B: ClassTag](it: Iterator[A], size: Int)(
      f: Seq[A] => TraversableOnce[B]
  ): Iterator[B] = {
    it.grouped(size).flatMap(f)
  }

  override def fold[A: ClassTag](it: Iterator[A])(zero: A)(f: (A, A) => A): A = it.fold(zero)(f)

  override def reduce[A: ClassTag](it: Iterator[A])(f: (A, A) => A): A = it.reduce(f)

  override def reduceByKey[K: ClassTag, V: ClassTag](it: Iterator[(K, V)])(f: (V, V) => V): Iterator[(K, V)] = {
    CollectionUtils.reduceByKey(it, reduceLimit)(f)
  }

  override def limit[A: ClassTag](it: Iterator[A])(n: Int): Iterator[A] = it.take(n)

  override def materialize[A: ClassTag](it: Iterator[A]): Seq[A] = it.toSeq

}

object MapReducibleBase {
  val iteratorMR: MapReducible[Iterator] = new MapReducibleBase()
}
