package org.yupana.core.dao

import org.yupana.api.query.Condition
import org.yupana.api.schema.Dimension
import org.yupana.core.model.{InternalQuery, InternalRow, InternalRowBuilder}
import org.yupana.core.utils.metric.MetricQueryCollector

import scala.language.higherKinds

trait TSReadingDao[Collection[_], IdType] {
  def query(query: InternalQuery, valueDataBuilder: InternalRowBuilder, metricCollector: MetricQueryCollector): Collection[InternalRow]

  def idsToValues(dimension: Dimension, ids: Set[Long]): Map[Long, String]
  def valuesToIds(dimension: Dimension, values: Set[String]): Map[String, Long]

  def isSupportedCondition(condition: Condition): Boolean
}
