package org.yupana.core.dao

trait InvertedIndexDao[K, V] {
  def put(key: K, values: Set[V]): Unit
  def batchPut(batch: Map[K, Set[V]]): Unit
  def values(key: K): Iterator[V]
  def allValues(keys: Set[K]): Seq[V]
}
