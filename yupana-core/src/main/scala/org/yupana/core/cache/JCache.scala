package org.yupana.core.cache

import java.util

import javax.cache.{Cache => JavaCache}

import scala.collection.JavaConverters._

class JCache[K, V](underlying: JavaCache[K, V]) extends Cache[K, V] {
  override def get(key: K): Option[V] = Option(underlying.get(key))
  override def put(key: K, value: V): Unit = underlying.put(key, value)
  override def remove(key: K): Boolean = underlying.remove(key)
  override def getAll(keys: Set[K]): Map[K, V] = underlying.getAll(keys.asJava).asScala.toMap
  override def removeAll(): Unit = underlying.clear()
  override def contains(key: K): Boolean = underlying.containsKey(key)

  override def putAll(batch: Map[K, V]): Unit = {
    val treeMap = new util.TreeMap[K,V]()
    treeMap.putAll(batch.asJava)
    underlying.putAll(treeMap)
  }
}
