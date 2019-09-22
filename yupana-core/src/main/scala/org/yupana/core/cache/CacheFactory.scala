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

import java.util.Properties

import com.typesafe.scalalogging.StrictLogging
import org.yupana.ignite.cache.IgniteCacheFactory
import org.yupana.core.cache.CacheFactory.CacheEngine.CacheEngine
import org.yupana.ehcache.EhCacheFactory

import scala.collection.JavaConverters._

trait CacheFactory {
  def initCache(description: CacheDescription): Cache[description.Key, description.Value]
  def getCache(description: CacheDescription): Cache[description.Key, description.Value]
  def flushCaches(): Unit
}

object CacheFactory extends StrictLogging {

  object CacheEngine extends Enumeration {
    type CacheEngine = Value
    val EhCache, Ignite, Disabled = Value
  }

  private val factories = Map(
    CacheEngine.EhCache -> new EhCacheFactory,
    CacheEngine.Ignite -> new IgniteCacheFactory,
    CacheEngine.Disabled -> new DisabledCacheFactory
  )

  private var defaultEngine: CacheEngine = _
  private var properties: Properties = _
  private var nameSuffix: String = _

  def propsForPrefix(prefix: String): Map[String, String] = {
    properties.asScala.filter(_._1.startsWith(prefix + ".")).map { case (k, v) => k.drop(prefix.length + 1) -> v }.toMap
  }

  def createDescription[K: BoxingTag, V: BoxingTag](name: String): CacheDescription.Aux[K, V] = {
    if (properties == null) throw new IllegalStateException("CacheUtils were not properly initialized")
    val props = propsForPrefix("analytics.caches." + name)
    val engine = props
      .get("engine")
      .map(
        engineName =>
          CacheEngine.values
            .find(_.toString == engineName)
            .getOrElse(throw new IllegalArgumentException(s"Unknown cache engine $engineName for cache $name"))
      )
      .getOrElse(defaultEngine)

    CacheDescription(name, nameSuffix, engine)
  }

  def init(properties: Properties, hbaseNamespace: String): Unit = {
    if (this.properties == null) {
      this.properties = properties
      val defaultEngineName = Option(properties.getProperty("analytics.caches.default.engine"))
        .getOrElse(throw new IllegalArgumentException("Default cache engine is not defined"))

      defaultEngine = CacheEngine.values
        .find(_.toString == defaultEngineName)
        .getOrElse(throw new IllegalArgumentException(s"Unknown default cache engine $defaultEngineName"))

      nameSuffix = Option(properties.getProperty("analytics.caches.default.suffix"))
        .filter(_.trim.nonEmpty)
        .getOrElse(hbaseNamespace)
        .trim
    } else logger.info("CacheUtils already initialized")
  }

  def initCache[K, V](description: CacheDescription.Aux[K, V]): Cache[K, V] = synchronized {
    if (properties == null) throw new IllegalStateException("CacheUtils was not initialized")
    logger.debug(s"Initialize cache ${description.name}")
    getFactory(description).initCache(description)
  }

  def initCache[K: BoxingTag, V: BoxingTag](name: String): Cache[K, V] = {
    initCache(createDescription[K, V](name))
  }

  def flushCaches(): Unit = {
    logger.info("Flushing caches")
    factories.values.foreach(_.flushCaches())
  }

  private def getFactory(description: CacheDescription): CacheFactory = {
    factories.getOrElse(
      description.engine,
      throw new IllegalArgumentException(s"Unsupported cache engine ${description.engine}")
    )
  }
}
