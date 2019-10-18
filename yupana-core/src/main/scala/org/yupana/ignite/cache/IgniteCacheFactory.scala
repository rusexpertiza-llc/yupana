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

package org.yupana.ignite.cache

import com.typesafe.scalalogging.StrictLogging
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicyFactory
import org.apache.ignite.configuration.{ CacheConfiguration, IgniteConfiguration, NearCacheConfiguration }
import org.apache.ignite.logger.slf4j.Slf4jLogger
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import org.apache.ignite.{ Ignite, Ignition }
import org.yupana.core.cache.{ Cache, CacheDescription, CacheFactory, JCache }

import scala.collection.JavaConverters._

class IgniteCacheFactory extends CacheFactory with StrictLogging {
  private var ignite: Option[Ignite] = None

  private var caches = Set.empty[CacheDescription]
  private var defaultCacheSize: Int = _

  override def initCache(description: CacheDescription): Cache[description.Key, description.Value] = {
    logger.info(s"Initializing cache ${description.fullName} in Ignite")
    if (ignite.isEmpty) initIgnite()

    ignite match {
      case Some(i) =>
        if (!caches.contains(description)) {
          val (cacheConfig, nearConfig) = createConfig(description)
          val cache = i.getOrCreateCache(cacheConfig, nearConfig)
          caches += description
          new JCache(cache)
        } else {
          logger.debug(s"Cache ${description.fullName} already exists")
          getCache(description)
        }

      case None => throw new IllegalStateException("Cannot initialize Ignite client")
    }
  }

  private def initIgnite(): Unit = {
    val props = CacheFactory.propsForPrefix("analytics.caches.default.ignite")
    val config = new IgniteConfiguration()

    config.setGridLogger(new Slf4jLogger())

    props.get("servers") match {
      case Some(servers) =>
        val serverList = servers.split(",").map(_.trim).toList
        val ipFinder = new TcpDiscoveryVmIpFinder()
        ipFinder.setAddresses(serverList.asJava)
        val discoSpi = new TcpDiscoverySpi
        discoSpi.setIpFinder(ipFinder)
        config.setDiscoverySpi(discoSpi)

      case None =>
        logger.info("Zookeeper is not set for Ignite, using default discovery")
    }

    defaultCacheSize =
      props.getOrElse("maxSize", throw new IllegalArgumentException("Default max cache size is not defined")).toInt

    config.setClientMode(true)
    ignite = Some(Ignition.start(config))
  }

  override def getCache(description: CacheDescription): Cache[description.Key, description.Value] = {
    ignite match {
      case Some(i) =>
        if (caches.contains(description)) {
          new JCache(i.getOrCreateCache[description.Key, description.Value](description.fullName))
        } else {
          throw new IllegalStateException(s"Cache ${description.fullName} was not initialized")
        }

      case None =>
        throw new IllegalStateException("Ignite is not initialized")
    }
  }

  override def flushCaches(): Unit = {
    caches.foreach(d => getCache(d).removeAll())
  }

  private def createConfig(
      description: CacheDescription
  ): (
      CacheConfiguration[description.Key, description.Value],
      NearCacheConfiguration[description.Key, description.Value]
  ) = {
    val props = CacheFactory.propsForPrefix("analytics.caches." + description.name)

    val config = new CacheConfiguration[description.Key, description.Value](description.fullName)

    val maxSize = props.get("maxSize").map(_.toInt).getOrElse(defaultCacheSize)
//    config.setEvictionPolicyFactory(new LruEvictionPolicyFactory(0, 1, maxSize))

    val nearSize = props.get("nearSize").map(_.toInt).getOrElse(maxSize / 10)
    val nearConfig = new NearCacheConfiguration[description.Key, description.Value]()
    nearConfig.setNearEvictionPolicyFactory(new LruEvictionPolicyFactory(0, 1, nearSize))

    config.setNearConfiguration(nearConfig)
    config.setStatisticsEnabled(true)
    config.setCopyOnRead(false)

//    val ttl = props.get("timeToLive").map(_.toInt)
//    val tti = props.get("timeToIdle").map(_.toInt)
//
//    (ttl, tti) match {
//      case (_, Some(i)) => config.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, i)))
//      case (Some(l), _) => config.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, l)))
//      case _ =>
//    }

    (config, nearConfig)
  }
}
