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

import org.yupana.core.cache.CacheFactory.CacheEngine.CacheEngine

abstract class CacheDescription(val name: String, val suffix: String, val engine: CacheEngine) {
  type Key
  def keyBoxing: BoxingTag[Key]

  type Value
  def valueBoxing: BoxingTag[Value]

  val fullName: String = s"${name}_$suffix"

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case that: CacheDescription => this.name == that.name && this.suffix == that.suffix && this.engine == that.engine
      case _                      => false
    }
  }

  override def hashCode(): Int = {
    ((37 * 17 + name.hashCode) * 17 + suffix.hashCode) * 17 + engine.hashCode()
  }

}

object CacheDescription {
  type Aux[K, V] = CacheDescription { type Key = K; type Value = V }

  def apply[K, V](name: String, suffix: String, engine: CacheEngine)(
      implicit
      kTag: BoxingTag[K],
      vTag: BoxingTag[V]
  ): CacheDescription.Aux[K, V] = {
    new CacheDescription(name, suffix, engine) {
      override type Key = K
      override val keyBoxing: BoxingTag[K] = kTag

      override type Value = V
      override val valueBoxing: BoxingTag[V] = vTag
    }
  }
}
