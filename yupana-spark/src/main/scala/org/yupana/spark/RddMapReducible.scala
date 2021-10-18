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

package org.yupana.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.yupana.core.MapReducible
import org.yupana.core.utils.CloseableIterator
import org.yupana.core.utils.metric.MetricQueryCollector

import scala.collection.compat.IterableOnce
import scala.collection.compat.immutable.ArraySeq
import scala.reflect.ClassTag

class RddMapReducible(@transient val sparkContext: SparkContext, metricCollector: MetricQueryCollector)
    extends MapReducible[RDD]
    with Serializable {

  override def singleton[A: ClassTag](a: A): RDD[A] = sparkContext.parallelize(Seq(a))

  override def filter[A: ClassTag](rdd: RDD[A])(f: A => Boolean): RDD[A] = {
    val filtered = rdd.filter(f)
    saveMetricOnCompleteRdd(filtered)
  }

  override def map[A: ClassTag, B: ClassTag](rdd: RDD[A])(f: A => B): RDD[B] = {
    val mapped = rdd.map(f)
    saveMetricOnCompleteRdd(mapped)
  }

  override def flatMap[A: ClassTag, B: ClassTag](rdd: RDD[A])(f: A => Iterable[B]): RDD[B] = {
    val r = rdd.flatMap(f)
    saveMetricOnCompleteRdd(r)
  }

  override def batchFlatMap[A, B: ClassTag](rdd: RDD[A], size: Int)(f: Seq[A] => IterableOnce[B]): RDD[B] = {
    val r = rdd.mapPartitions(_.grouped(size).flatMap(f))
    saveMetricOnCompleteRdd(r)
  }

  override def fold[A: ClassTag](rdd: RDD[A])(zero: A)(f: (A, A) => A): A = {
    saveMetricOnCompleteRdd(rdd).fold(zero)(f)
  }

  override def reduce[A: ClassTag](rdd: RDD[A])(f: (A, A) => A): A = {
    saveMetricOnCompleteRdd(rdd).reduce(f)
  }

  override def reduceByKey[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)])(f: (V, V) => V): RDD[(K, V)] = {
    val r = rdd.reduceByKey(f)
    saveMetricOnCompleteRdd(r)
  }

  override def distinct[A: ClassTag](rdd: RDD[A]): RDD[A] = {
    val r = rdd.distinct()
    saveMetricOnCompleteRdd(r)
  }

  override def limit[A: ClassTag](c: RDD[A])(n: Int): RDD[A] = {
    val rdd = saveMetricOnCompleteRdd(c)
    val r = sparkContext.parallelize(ArraySeq.unsafeWrapArray(rdd.take(n)))
    saveMetricOnCompleteRdd(r)
  }

  override def materialize[A: ClassTag](c: RDD[A]): Seq[A] = ArraySeq.unsafeWrapArray(c.collect())

  private def saveMetricOnCompleteRdd[A: ClassTag](rdd: RDD[A]): RDD[A] = {
    rdd.mapPartitionsWithIndex { (id, it) =>
      CloseableIterator[A](it, metricCollector.checkpoint())
    }
  }
}
