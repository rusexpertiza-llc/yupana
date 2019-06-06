package org.yupana.core

import scala.language.higherKinds
import scala.reflect.ClassTag

/**
  * Defines basic operations on [[Collection]]
  */
trait MapReducible[Collection[_]] extends Serializable {
  def filter[A](c: Collection[A])(f: A => Boolean): Collection[A]

  def map[A, B : ClassTag](c: Collection[A])(f: A => B): Collection[B]
  def flatMap[A, B : ClassTag](mr: Collection[A])(f: A => Iterable[B]): Collection[B]

  def batchFlatMap[A, B : ClassTag](c: Collection[A])(size: Int, f: Seq[A] => Seq[B]): Collection[B]

  def fold[A](c: Collection[A])(zero: A)(f: (A, A) => A): A
  def reduce[A](c: Collection[A])(f: (A, A) => A): A
  def reduceByKey[K : ClassTag, V : ClassTag](c: Collection[(K, V)])(f: (V, V) => V): Collection[(K, V)]

  def limit[A: ClassTag](c: Collection[A])(n: Int): Collection[A]
}

object MapReducible {
  val iteratorMR: MapReducible[Iterator] = new MapReducible[Iterator] {
    override def filter[A](it: Iterator[A])(f: A => Boolean): Iterator[A] = it.filter(f)

    override def map[A, B : ClassTag](it: Iterator[A])(f: A => B): Iterator[B] = it.map(f)
    override def flatMap[A, B : ClassTag](it: Iterator[A])(f: A => Iterable[B]): Iterator[B] = it.flatMap(f)

    override def batchFlatMap[A, B: ClassTag](it: Iterator[A])(size: Int, f: Seq[A] => Seq[B]): Iterator[B] = {
      it.sliding(size, size).flatMap(f)
    }

    override def fold[A](it: Iterator[A])(zero: A)(f: (A, A) => A): A = it.fold(zero)(f)

    override def reduce[A](it: Iterator[A])(f: (A, A) => A): A = it.reduce(f)

    override def reduceByKey[K : ClassTag, V : ClassTag](it: Iterator[(K, V)])(f: (V, V) => V): Iterator[(K, V)] = {
      val map = collection.mutable.Map.empty[K, V]
      it.foreach { case (k, v) =>
        val old = map.get(k)
        val n = old.map(o => f(o, v)).getOrElse(v)
        map.put(k, n)
      }
      map.iterator
    }

    override def limit[A: ClassTag](it: Iterator[A])(n: Int): Iterator[A] = it.take(n)
  }
}
