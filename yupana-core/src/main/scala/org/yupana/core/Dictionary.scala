package org.yupana.core

import com.typesafe.scalalogging.StrictLogging
import org.yupana.api.schema.Dimension
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.DictionaryDao

class Dictionary(dimension: Dimension, dao: DictionaryDao) extends StrictLogging {
  private val cache = CacheFactory.initCache[Long, String](s"dictionary-${dimension.name}")
  private val absentsCache = CacheFactory.initCache[String, Boolean](s"dictionary-absents-${dimension.name}")
  private val reverseCache = CacheFactory.initCache[String, Long](s"dictionary-reverse-${dimension.name}")

  def value(id: Long): Option[String] = {
    cache.get(id) match {
      case Some(v) => Some(v)
      case None => if (isMarkedAsAbsent(id)) {
        None
      } else {
        logger.trace(s"Get value for id $id, dictionary ${dimension.name}")
        dao.getValueById(dimension, id).fold[Option[String]] {
          markAsAbsent(id)
          None
        }{ value =>
          updateCache(id, value)
          Some(value)
        }
      }
    }
  }

  def values(ids: Set[Long]): Map[Long, String] = {
    val fromCache = cache.getAll(ids)

    val idsToGet = ids.filter(id =>
      fromCache.get(id).isEmpty && !isMarkedAsAbsent(id)
    )

    val fromDB = if (idsToGet.nonEmpty) {
      val gotValues = dao.getValuesByIds(dimension, idsToGet)

      idsToGet.foreach { id =>
        if (gotValues.get(id).isEmpty) {
          markAsAbsent(id)
        }
      }

      cache.putAll(gotValues)

      gotValues
    } else {
      Map.empty
    }
    fromCache ++ fromDB
  }

  def findIdByValue(value: String): Option[Long] = {
    val trimmed = trimValue(value)
    reverseCache.get(trimmed).orElse {
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
      dao.getIdByValue(dimension, v).getOrElse(
        throw new IllegalStateException(s"Can't put value to $v dictionary ${dimension.name}")
      )
    }
    updateReverseCache(v, id)
    id
  }

  private def getIdsByValues(trimmed: Set[String]): Map[String, Long] = {
    val fromCache = reverseCache.getAll(trimmed)
    val notInCacheValues = trimmed -- fromCache.keySet

    if (notInCacheValues.nonEmpty) {
      val fromDao = dao.getIdsByValues(dimension, notInCacheValues)
      reverseCache.putAll(fromDao)
      fromDao ++ fromCache
    } else {
      fromCache
    }
  }

  private def updateCache(id: Long, value: String): Unit = {
    cache.put(id, value)
  }

  private def updateReverseCache(normValue: String, id: Long): Unit = {
    reverseCache.put(normValue, id)
  }

  private def markAsAbsent(id: Long): Unit = {
    absentsCache.put(id.toString, true)
  }

  private def isMarkedAsAbsent(id: Long): Boolean = {
    absentsCache.get(id.toString).getOrElse(false)
  }

  private def generateId(): Int = {
    dao.createSeqId(dimension)
  }

  private def trimValue(value: String): String = {
    if (value != null) value.trim else ""
  }
}
