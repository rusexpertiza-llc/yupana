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

package org.yupana.ehcache

import com.typesafe.scalalogging.StrictLogging

import javax.cache.configuration.Configuration
import javax.cache.{ CacheManager, Caching }
import org.ehcache.config.CacheConfiguration
import org.ehcache.config.builders.{ CacheConfigurationBuilder, ExpiryPolicyBuilder, ResourcePoolsBuilder }
import org.ehcache.config.units.{ EntryUnit, MemoryUnit }
import org.ehcache.jsr107.Eh107Configuration
import org.yupana.core.cache.{ Cache, CacheDescription, CacheFactory, JCache }

import java.time.Duration

class EhCacheFactory extends CacheFactory with StrictLogging {

  private var cacheManager: Option[CacheManager] = None
  private var caches = Set.empty[CacheDescription]

  override val name: String = "EhCache"

  override def initCache(description: CacheDescription): Cache[description.Key, description.Value] = {
    logger.info(s"Initializing cache ${description.fullName} in EhCache")
    if (cacheManager.isEmpty) initManager()

    cacheManager match {
      case Some(cm) =>
        if (!caches.contains(description)) {
          logger.info(s"Adding cache ${description.fullName}")
          val cache =
            cm.createCache[description.Key, description.Value, Configuration[description.Key, description.Value]](
              description.fullName,
              Eh107Configuration.fromEhcacheCacheConfiguration(createCacheConfig(description))
            )

          caches += description
          new JCache(cache)
        } else {
          logger.debug(s"Cache ${description.fullName} already exists, skipping")
          getCache(description)
        }
      case None =>
        throw new IllegalStateException("Cannot initialize EhCache manager")
    }
  }

  override def getCache(description: CacheDescription): Cache[description.Key, description.Value] = {
    cacheManager match {
      case Some(cm) => new JCache(cm.getCache[description.Key, description.Value](description.fullName))
      case None     => throw new IllegalStateException("EhCache manager is not initialized yet")
    }
  }

  override def flushCaches(): Unit = {
    caches.foreach(d => getCache(d).removeAll())
  }

  protected def initManager(): Unit = {
    if (cacheManager.isEmpty) {
      logger.info("Initializing EhCache cache manager")
      System.setProperty(
        Caching.JAVAX_CACHE_CACHING_PROVIDER,
        classOf[org.ehcache.jsr107.EhcacheCachingProvider].getName
      )
      val provider = Caching.getCachingProvider(classOf[org.ehcache.jsr107.EhcacheCachingProvider].getName)
      cacheManager = Some(provider.getCacheManager)
    }
  }

  private def createCacheConfig(
      description: CacheDescription
  ): CacheConfiguration[description.Key, description.Value] = {
    val props = CacheFactory.propsForPrefix("analytics.caches." + description.name)
    val defaultProps = CacheFactory.propsForPrefix("analytics.caches.default.ehcache")

    val eternal = props.getOrElse("eternal", "false").toBoolean
    val expiry = if (eternal) {
      ExpiryPolicyBuilder.noExpiration()
    } else {
      val ttl = Duration.ofSeconds(
        props.get("timeToLive").orElse(defaultProps.get("timeToLive")).map(_.toLong).getOrElse(DEFAULT_TTL)
      )
      val tti = props.get("timeToIdle").map(s => Duration.ofSeconds(s.toLong)).orNull
      ExpiryPolicyBuilder.expiry().create(ttl).access(tti).update(ttl).build()
    }

    val resourcePoolsBuilder = createResourcePool(props)
      .orElse(createResourcePool(defaultProps))
      .getOrElse(throw new IllegalArgumentException(s"Cache size is not defined for ${description.name}"))

    val conf = CacheConfigurationBuilder
      .newCacheConfigurationBuilder(description.keyBoxing.clazz, description.valueBoxing.clazz, resourcePoolsBuilder)
      .withExpiry(expiry)
      .build()

    conf.asInstanceOf[CacheConfiguration[description.Key, description.Value]]
  }

  private def createResourcePool(props: Map[String, String]): Option[ResourcePoolsBuilder] = {
    val elements = props.get("maxElements").map(_.toLong)
    val heapSize = props.get("heapSize").map(_.toLong)
    val offHeapSize = props.get("offHeapSize").map(_.toLong)

    elements match {
      case Some(e) =>
        Some(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(e, EntryUnit.ENTRIES))

      case None if heapSize.nonEmpty || offHeapSize.nonEmpty =>
        val builder = ResourcePoolsBuilder.newResourcePoolsBuilder()
        val withHeap = heapSize.map(b => builder.heap(b, MemoryUnit.B)).getOrElse(builder)
        Some(offHeapSize.map(b => withHeap.offheap(b, MemoryUnit.B)).getOrElse(withHeap))

      case None =>
        None
    }
  }
}
