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

import javax.cache.expiry.Duration
import java.util.OptionalLong
import java.util.concurrent.TimeUnit.SECONDS

import com.github.benmanes.caffeine.jcache.configuration.CaffeineConfiguration
import com.typesafe.scalalogging.StrictLogging
import javax.cache.configuration.Configuration
import javax.cache.expiry.CreatedExpiryPolicy
import javax.cache.{ CacheManager, Caching }
import org.yupana.core.cache.{ Cache, CacheDescription, CacheFactory, JCache }

class CaffeineCacheFactory extends CacheFactory with StrictLogging {

  private var cacheManager: CacheManager = _

  private var caches = Set.empty[CacheDescription]

  override val name: String = "Caffeine"

  override def initCache(description: CacheDescription): Cache[description.Key, description.Value] = {
    init()

    if (!caches.contains(description)) {
      val config = createCacheConfig(description)
      caches += description
      new JCache(cacheManager.createCache(description.fullName, config))
    } else {
      logger.debug(s"Cache ${description.fullName} already exists, skipping")
      getCache(description)
    }
  }

  override def getCache(description: CacheDescription): Cache[description.Key, description.Value] = {
    if (cacheManager != null) {
      new JCache(
        cacheManager.getCache(description.fullName, description.keyBoxing.clazz, description.valueBoxing.clazz)
      ).asInstanceOf[Cache[description.Key, description.Value]]
    } else {
      throw new IllegalStateException("Caffeine manager is not initialized yet")
    }
  }

  override def flushCaches(): Unit = {
    caches.foreach(d => getCache(d).removeAll())
  }

  private def init(): Unit = {
    if (cacheManager == null) {
      logger.info("Initializing Caffeine cache manager")
      System.setProperty(
        Caching.JAVAX_CACHE_CACHING_PROVIDER,
        classOf[com.github.benmanes.caffeine.jcache.spi.CaffeineCachingProvider].getName
      )

      val provider =
        Caching.getCachingProvider(classOf[com.github.benmanes.caffeine.jcache.spi.CaffeineCachingProvider].getName)
      cacheManager = provider.getCacheManager(provider.getDefaultURI, provider.getDefaultClassLoader)
    }
  }

  private def createCacheConfig(description: CacheDescription): Configuration[description.Key, description.Value] = {
    val configuration = new CaffeineConfiguration[description.Key, description.Value]

    configuration.setTypes(
      description.keyBoxing.clazz.asInstanceOf[Class[description.Key]],
      description.valueBoxing.clazz.asInstanceOf[Class[description.Value]]
    )
    configuration.setStatisticsEnabled(true)

    val props = CacheFactory.propsForPrefix("analytics.caches." + description.name)
    val defaultProps = CacheFactory.propsForPrefix("analytics.caches.default.caffeine")

    val eternal = props.getOrElse("eternal", "false").toBoolean
    val expiry = if (eternal) {
      Duration.ETERNAL
    } else {
      val timeToLive =
        props.get("timeToLive").orElse(defaultProps.get("timeToLive")).map(_.toLong).getOrElse(DEFAULT_TTL)
      new Duration(SECONDS, timeToLive)
    }
    configuration.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(expiry))

    val maxSize = props
      .get("maxElements")
      .orElse(defaultProps.get("maxElements"))
      .map(_.toLong)
      .fold(OptionalLong.empty())(x => OptionalLong.of(x))

    configuration.setMaximumSize(maxSize)

    configuration
  }
}
