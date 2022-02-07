package org.yupana.lmdb

import org.yupana.api.query.Query
import org.yupana.api.schema.{ Dimension, Schema }
import org.yupana.core.dao.{ ChangelogDao, DictionaryDao, DictionaryProviderImpl }
import org.yupana.core.model.UpdateInterval
import org.yupana.core.{ TSDB, TsdbConfig }
import org.yupana.core.utils.metric.{ MetricQueryCollector, NoMetricCollector }

object TSDBLmdb {

  def apply(
      schema: Schema,
      prepareQuery: Query => Query,
      tsdbConfig: TsdbConfig,
      metricCollectorCreator: Query => MetricQueryCollector = _ => NoMetricCollector
  ): TSDB = {

    val dao = new TSDaoLmdb(schema)
    val changeLogDao = new ChangelogDao {
      override def putUpdatesIntervals(intervals: Seq[UpdateInterval]): Unit = ()
      override def getUpdatesIntervals(
          tableName: Option[String],
          updatedAfter: Option[Long],
          updatedBefore: Option[Long],
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
