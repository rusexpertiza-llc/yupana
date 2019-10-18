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

package org.yupana.core.cache

import java.util

import javax.cache.{ Cache => JavaCache }

import scala.collection.JavaConverters._

class JCache[K, V](underlying: JavaCache[K, V]) extends Cache[K, V] {
  override def get(key: K): Option[V] = Option(underlying.get(key))
  override def getNullable(key: K): V = underlying.get(key)
  override def put(key: K, value: V): Unit = underlying.put(key, value)
  override def remove(key: K): Boolean = underlying.remove(key)
  override def getAll(keys: Set[K]): Map[K, V] = underlying.getAll(keys.asJava).asScala.toMap
  override def removeAll(): Unit = underlying.clear()
  override def contains(key: K): Boolean = underlying.containsKey(key)

  override def putAll(batch: Map[K, V]): Unit = {
    val treeMap = new util.TreeMap[K, V]()
    treeMap.putAll(batch.asJava)
    underlying.putAll(treeMap)
  }
}
