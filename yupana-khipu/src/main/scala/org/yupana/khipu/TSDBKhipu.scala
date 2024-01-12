package org.yupana.khipu

import org.yupana.api.query.Query
import org.yupana.api.schema.Schema
import org.yupana.cache.CacheFactory
import org.yupana.core.dao.ChangelogDao
import org.yupana.core.{ TSDB, TsdbConfig }
import org.yupana.core.utils.metric.{ MetricQueryCollector, NoMetricCollector }
import org.yupana.settings.Settings

object TSDBKhipu {

  def apply(
      schema: Schema,
      prepareQuery: Query => Query,
      tsdbConfig: TsdbConfig,
      settings: Settings,
      changelogDao: ChangelogDao,
      metricCollectorCreator: Query => MetricQueryCollector = _ => NoMetricCollector
  ): TSDB = {
    CacheFactory.init(settings)

    val dao = new TSDaoKhipu(schema, settings)

    new TSDB(
      schema,
      dao,
      changelogDao,
      prepareQuery,
      tsdbConfig,
      metricCollectorCreator
    )
  }
}
