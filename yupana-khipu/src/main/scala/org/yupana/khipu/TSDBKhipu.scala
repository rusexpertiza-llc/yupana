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
      metricCollectorCreator: (Query, String) => MetricQueryCollector = (_,_) => NoMetricCollector

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
