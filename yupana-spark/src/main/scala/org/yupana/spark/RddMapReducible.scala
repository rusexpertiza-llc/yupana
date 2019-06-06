package org.yupana.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.yupana.core.MapReducible

import scala.reflect.ClassTag

class RddMapReducible(@transient val sparkContext: SparkContext) extends MapReducible[RDD] with Serializable {
  override def filter[A](rdd: RDD[A])(f: A => Boolean): RDD[A] = rdd.filter(f)

  override def map[A, B : ClassTag](rdd: RDD[A])(f: A => B): RDD[B] = rdd.map(f)
  override def flatMap[A, B : ClassTag](rdd: RDD[A])(f: A => Iterable[B]): RDD[B] = rdd.flatMap(f)

  override def batchFlatMap[A, B: ClassTag](rdd: RDD[A])(size: Int, f: Seq[A] => Seq[B]): RDD[B] = {
    rdd.mapPartitions(_.sliding(size, size).flatMap(f))
  }

  override def fold[A](rdd: RDD[A])(zero: A)(f: (A, A) => A): A = rdd.fold(zero)(f)

  override def reduce[A](rdd: RDD[A])(f: (A, A) => A): A = rdd.reduce(f)
  override def reduceByKey[K : ClassTag, V : ClassTag](rdd: RDD[(K, V)])(f: (V, V) => V): RDD[(K, V)] = {
    val numPartitions = math.max(rdd.partitions.length, 50)
    rdd.reduceByKey(f, numPartitions)
  }

  override def limit[A : ClassTag](c: RDD[A])(n: Int): RDD[A] = {
    sparkContext.parallelize(c.take(n))
  }
}
