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

package org.yupana.core

import com.typesafe.scalalogging.StrictLogging
import org.yupana.api.schema.DictionaryDimension
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.DictionaryDao

class Dictionary(dimension: DictionaryDimension, dao: DictionaryDao) extends StrictLogging {
  private val cache = CacheFactory.initCache[String, Long](s"dictionary-${dimension.name}")

  def findIdByValue(value: String): Option[Long] = {
    val trimmed = trimValue(value)
    cache.get(trimmed).orElse {
      dao.getIdByValue(dimension, trimmed).map { id =>
        updateReverseCache(trimmed, id)
        id
      }
    }
  }

  def findIdsByValues(values: Set[String]): Map[String, Long] = {
    val trimmed = values.map(v => v -> trimValue(v)).toMap
    if (trimmed.nonEmpty) {
      val ids = getIdsByValues(trimmed.values.toSet)
      trimmed.flatMap { case (v, t) => ids.get(t).map(v -> _) }
    } else {
      Map.empty
    }
  }

  def id(value: String): Long = {
    val v = trimValue(value)
    findIdByValue(v).getOrElse {
      put(v)
    }
  }

  def getOrCreateIdsForValues(values: Set[String]): Map[String, Long] = {
    val trimmed = values.map(trimValue)
    val exists = getIdsByValues(trimmed)
    val notExists = trimmed -- exists.keySet

    val added = notExists.map(v => v -> put(v)).toMap

    added ++ exists
  }

  private def put(v: String): Long = {
    val hash = dimension.hash(v)

    val genId = (hash.toLong << 32) | generateId()
    val id = if (dao.checkAndPut(dimension, genId, v)) {
      genId
    } else {
      dao
        .getIdByValue(dimension, v)
        .getOrElse(
          throw new IllegalStateException(s"Can't put value $v to dictionary ${dimension.name}")
        )
    }
    updateReverseCache(v, id)
    id
  }

  private def getIdsByValues(trimmed: Set[String]): Map[String, Long] = {
    val fromCache = cache.getAll(trimmed)
    val notInCacheValues = trimmed -- fromCache.keySet

    if (notInCacheValues.nonEmpty) {
      val fromDao = dao.getIdsByValues(dimension, notInCacheValues)
      cache.putAll(fromDao)
      fromDao ++ fromCache
    } else {
      fromCache
    }
  }

  private def updateReverseCache(normValue: String, id: Long): Unit = {
    cache.put(normValue, id)
  }

  private def generateId(): Int = {
    dao.createSeqId(dimension)
  }

  private def trimValue(value: String): String = {
    if (value != null) value.trim else ""
  }
}
