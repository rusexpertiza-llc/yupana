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

import com.github.benmanes.caffeine.cache.{ Cache => CCache, Caffeine }

import com.typesafe.scalalogging.StrictLogging
import org.yupana.cache.{ Cache, CacheDescription, CacheFactory }

import java.util.concurrent.TimeUnit

class CaffeineCacheFactory extends CacheFactory with StrictLogging {

  private var caches = Map.empty[CacheDescription, Cache[_, _]]

  override val name: String = "Caffeine"

  override def initCache(description: CacheDescription): Cache[description.Key, description.Value] = {
    init()

    if (!caches.contains(description)) {
      val settings = CacheFactory.settings.inner(s"analytics.caches.${description.name}.")
      val defaultSettings = CacheFactory.settings.inner("analytics.caches.default.caffeine.")
      val builder = Caffeine
        .newBuilder()
        .recordStats()

      settings
        .opt[Long]("maxElements")
        .orElse(settings.opt[Long]("maxElements"))
        .foreach(maxSize => builder.maximumSize(maxSize))

      val eternal = settings[Boolean]("eternal", false)
      val (expiry, idle) = if (eternal) {
        (TimeUnit.MILLISECONDS.toNanos(Long.MaxValue), None)
      } else {
        val ttl = settings("timeToLive", defaultSettings("timeToLive", DEFAULT_TTL))
        val tti = settings.opt[Long]("timeToIdle").orElse(defaultSettings.opt[Long]("timeToIdle"))

        (TimeUnit.SECONDS.toNanos(ttl), tti.map(TimeUnit.SECONDS.toNanos))
      }

      builder.expireAfterWrite(expiry, TimeUnit.NANOSECONDS)
      idle.foreach(i => builder.expireAfterAccess(i, TimeUnit.NANOSECONDS))

      val cache = new CaffeineCache(
        builder
          .build[description.keyBoxing.R, description.valueBoxing.R]()
          .asInstanceOf[CCache[description.Key, description.Value]]
      )

      caches += description -> cache
      cache
    } else {
      logger.debug(s"Cache ${description.fullName} already exists, skipping")
      getCache(description)
    }
  }

  override def getCache(description: CacheDescription): Cache[description.Key, description.Value] = {
    caches.getOrElse(description, null).asInstanceOf[Cache[description.Key, description.Value]]
  }

  override def flushCaches(): Unit = {
    caches.keys.foreach(d => getCache(d).removeAll())
  }

  private def init(): Unit = {}
}
