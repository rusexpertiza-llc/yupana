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
