package org.yupana.core

import org.yupana.cache.{ Cache, CacheDescription, CacheFactory }

class MapCache[K, V] extends Cache[K, V] {
  private var map = Map.empty[K, V]

  override def get(key: K): Option[V] = map.get(key)
  override def getNullable(key: K): V = map.getOrElse(key, null.asInstanceOf[V])
  override def put(key: K, value: V): Unit = map += key -> value
  override def remove(key: K): Boolean =
    if (map.contains(key)) {
      map -= key
      true
    } else false

  override def removeAll(): Unit = map = Map.empty
  override def getAll(keys: Set[K]): Map[K, V] = keys.flatMap(k => map.get(k).map(k -> _)).toMap
  override def putAll(batch: Map[K, V]): Unit = map ++= batch
  override def contains(key: K): Boolean = map.contains(key)
}

class MapCacheFactory extends CacheFactory {

  private var caches = Map.empty[String, MapCache[_, _]]

  override def name: String = "MapCache"

  override def initCache(description: CacheDescription): Cache[description.Key, description.Value] = {
    if (!caches.contains(description.name)) {
      val c = new MapCache[description.Key, description.Value]
      caches += name -> c
      c
    } else {
      caches(name).asInstanceOf[Cache[description.Key, description.Value]]
    }
  }

  override def getCache(description: CacheDescription): Cache[description.Key, description.Value] = {
    caches(name).asInstanceOf[Cache[description.Key, description.Value]]
  }

  override def flushCaches(): Unit = caches.foreach(_._2.removeAll())
}
