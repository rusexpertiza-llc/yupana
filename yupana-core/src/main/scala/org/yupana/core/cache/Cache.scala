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

trait Cache[K, V] {
  def get(key: K): Option[V]
  def getNullable(key: K): V
  def put(key: K, value: V): Unit
  def remove(key: K): Boolean
  def removeAll(): Unit
  def getAll(keys: Set[K]): Map[K, V]
  def putAll(batch: Map[K, V]): Unit
  def contains(key: K): Boolean

  def caching(key: K)(eval: => V): V = {
    val value = getNullable(key)
    if (value == null) {
      val v = eval
      put(key, v)
      v
    } else {
      value
    }
  }

  def allCaching(keys: Set[K])(eval: Set[K] => Map[K, V]): Map[K, V] = {
    val cached = getAll(keys)
    val missing = keys -- cached.keys
    val evaluated = if (missing.nonEmpty) {
      val e = eval(missing)
      putAll(e)
      e
    } else {
      Seq.empty
    }

    cached ++ evaluated
  }
}
