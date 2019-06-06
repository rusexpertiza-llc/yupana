package org.yupana.core.dao

import org.yupana.api.schema.Dimension

trait DictionaryDao {
  def createSeqId(dimension: Dimension): Int
  def getValueById(dimension: Dimension, id: Long): Option[String]
  def getValuesByIds(dimension: Dimension, ids: Set[Long]): Map[Long, String]
  def getIdByValue(dimension: Dimension, value: String): Option[Long]
  def getIdsByValues(dimension: Dimension, value: Set[String]): Map[String, Long]
  def checkAndPut(dimension: Dimension, id: Long, value: String): Boolean
}
