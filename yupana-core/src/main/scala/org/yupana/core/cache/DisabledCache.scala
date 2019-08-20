package org.yupana.core.cache

class DisabledCache[K, V] extends Cache[K, V] {
  override def getNullable(key: K): V = null.asInstanceOf[V]
  override def get(key: K): Option[V] = None
  override def put(key: K, value: V): Unit = {}
  override def remove(key: K): Boolean = false
  override def getAll(keys: Set[K]): Map[K, V] = Map.empty
  override def putAll(batch: Map[K, V]): Unit = {}
  override def removeAll(): Unit = {}
  override def contains(key: K): Boolean = false
}
