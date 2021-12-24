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

package org.yupana.caffeine

import com.github.benmanes.caffeine.cache.{ Cache => CCache }
import org.yupana.core.cache.Cache

import scala.jdk.CollectionConverters._

class CaffeineCache[K, V](cache: CCache[K, V]) extends Cache[K, V] {
  override def getNullable(key: K): V = cache.getIfPresent(key)

  override def put(key: K, value: V): Unit = cache.put(key, value)

  override def remove(key: K): Boolean = {
    cache.invalidate(key)
    true
  }

  override def removeAll(): Unit = cache.invalidateAll()

  override def getAll(keys: Set[K]): Map[K, V] = cache.getAllPresent(keys.asJava).asScala.toMap

  override def putAll(batch: Map[K, V]): Unit = cache.putAll(batch.asJava)

  override def contains(key: K): Boolean = cache.getIfPresent(key) != null
}
