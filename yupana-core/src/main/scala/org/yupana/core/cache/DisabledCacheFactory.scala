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

import com.typesafe.scalalogging.StrictLogging

class DisabledCacheFactory extends CacheFactory with StrictLogging {
  override def initCache(description: CacheDescription): Cache[description.Key, description.Value] = {
    logger.debug(s"Initializing cache ${description.name} in Disabled mode")
    new DisabledCache[description.Key, description.Value]()
  }
  override def getCache(description: CacheDescription): Cache[description.Key, description.Value] = {
    new DisabledCache[description.Key, description.Value]()
  }

  override def flushCaches(): Unit = {}
}
