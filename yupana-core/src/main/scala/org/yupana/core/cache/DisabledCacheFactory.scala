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
