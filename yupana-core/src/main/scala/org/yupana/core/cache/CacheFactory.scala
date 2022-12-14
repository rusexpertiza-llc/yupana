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
import org.yupana.api.types.BoxingTag
import org.yupana.core.settings.Settings

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

trait CacheFactory {

  val DEFAULT_TTL: Long = 30 * 60
  def name: String

  def initCache(description: CacheDescription): Cache[description.Key, description.Value]
  def getCache(description: CacheDescription): Cache[description.Key, description.Value]
  def flushCaches(): Unit
}

object CacheFactory extends StrictLogging {

  private var factories: Map[String, CacheFactory] = Map(
    "Disabled" -> new DisabledCacheFactory
  )

  private var defaultEngine: String = _
  var settings: Settings = _
  private var nameSuffix: Option[String] = None

  def createDescription[K: BoxingTag, V: BoxingTag](name: String): CacheDescription.Aux[K, V] = {
    if (settings == null) throw new IllegalStateException("CacheUtils were not properly initialized")
    val engine = settings.opt[String](s"analytics.caches.$name.engine") match {
      case Some(engineName) =>
        if (factories.contains(engineName)) {
          engineName
        } else {
          throw new IllegalArgumentException(s"Unknown cache engine $engineName for cache $name")
        }
      case None =>
        defaultEngine
    }

    CacheDescription(name, nameSuffix, engine)
  }

  def init(settings: Settings): Unit = {
    if (this.settings == null) {
      loadFactories()
      this.settings = settings
      val defaultEngineName = settings[String]("analytics.caches.default.engine")

      defaultEngine = if (factories.contains(defaultEngineName)) {
        defaultEngineName
      } else {
        throw new IllegalArgumentException(s"Unknown default cache engine $defaultEngineName")
      }

      nameSuffix = settings
        .opt[String]("analytics.caches.default.suffix")
        .map(_.trim)
        .filter(_.nonEmpty)

    } else logger.info("CacheUtils already initialized")
  }

  def initCache[K, V](description: CacheDescription.Aux[K, V]): Cache[K, V] = synchronized {
    if (settings == null) throw new IllegalStateException("CacheUtils was not initialized")
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

  def loadFactories(): Unit = {
    synchronized {
      ServiceLoader.load(classOf[CacheFactory]).asScala.foreach { factory =>
        val name = factory.name
        if (!factories.contains(name)) {
          logger.info(s"Registered cache factory $name, ${factory.getClass.getName}")
          factories += name -> factory
        } else {
          logger.warn(
            s"Multiple factories with name $name (${factory.getClass.getName} and ${factories(name).getClass.getName})"
          )
        }
      }
    }
  }

  private def getFactory(description: CacheDescription): CacheFactory = {
    factories.getOrElse(
      description.engine,
      throw new IllegalArgumentException(s"Unsupported cache engine ${description.engine}")
    )
  }
}
