package org.yupana.khipu

import org.yupana.api.query.Query
import org.yupana.api.schema.{ Dimension, Schema }
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.{ ChangelogDao, DictionaryDao, DictionaryProviderImpl }
import org.yupana.core.model.UpdateInterval
import org.yupana.core.{ TSDB, TsdbConfig }
import org.yupana.core.utils.metric.{ MetricQueryCollector, NoMetricCollector }

import java.util.Properties

object TSDBKhipu {

  def apply(
      schema: Schema,
      prepareQuery: Query => Query,
      tsdbConfig: TsdbConfig,
      properties: Properties,
      metricCollectorCreator: Query => MetricQueryCollector = _ => NoMetricCollector
  ): TSDB = {
    CacheFactory.init(properties)

    val dao = new TSDaoKhipu(schema)
    val changeLogDao = new ChangelogDao {
      override def putUpdatesIntervals(intervals: Seq[UpdateInterval]): Unit = ()

      override def getUpdatesIntervals(
          tableName: Option[String],
          updatedAfter: Option[Long],
          updatedBefore: Option[Long],
          recalculatedAfter: Option[Long],
          recalculatedBefore: Option[Long],
          updatedBy: Option[String]
      ): Iterable[UpdateInterval] = Seq.empty
    }

    val dictDao = new DictionaryDao {
      override def createSeqId(dimension: Dimension): Int = 0
      override def getIdByValue(dimension: Dimension, value: String): Option[Long] = None
      override def getIdsByValues(dimension: Dimension, value: Set[String]): Map[String, Long] = Map.empty
      override def checkAndPut(dimension: Dimension, id: Long, value: String): Boolean = true
    }

    val dictProvider = new DictionaryProviderImpl(dictDao)

    new TSDB(
      schema,
      dao,
      changeLogDao,
      dictProvider,
      prepareQuery,
      tsdbConfig,
      metricCollectorCreator
    )
  }
}
