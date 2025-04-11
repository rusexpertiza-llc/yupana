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

package org.yupana.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ Connection, ConnectionFactory }
import org.yupana.api.query.Query
import org.yupana.api.schema.Schema
import org.yupana.cache.CacheFactory
import org.yupana.core.dao.DictionaryProviderImpl
import org.yupana.core.utils.metric.{ MetricQueryCollector, PersistentMetricQueryReporter, StandaloneMetricCollector }
import org.yupana.core.{ TSDB, TsdbConfig }
import org.yupana.settings.Settings

object TSDBHBase {

  private def createDefaultMetricCollector(
      tsdbConfig: TsdbConfig,
      connection: Connection,
      namespace: String
  ): (Query, String) => MetricQueryCollector = {
    lazy val tsdbQueryMetricsDaoHBase = new TsdbQueryMetricsDaoHBase(connection, namespace)
    lazy val reporter = new PersistentMetricQueryReporter(tsdbQueryMetricsDaoHBase)

    { (query: Query, user: String) =>
      new StandaloneMetricCollector(
        query,
        user,
        "query",
        tsdbConfig.metricsUpdateInterval,
        reporter
      )
    }
  }

  def apply(
      connection: Connection,
      namespace: String,
      schema: Schema,
      prepareQuery: Query => Query,
      settings: Settings,
      tsdbConfig: TsdbConfig
  )(
      metricCollectorCreator: (Query, String) => MetricQueryCollector =
        createDefaultMetricCollector(tsdbConfig, connection, namespace)
  ): TSDB = {

    CacheFactory.init(settings)

    val dictDao = new DictionaryDaoHBase(connection, namespace)
    val dictProvider = new DictionaryProviderImpl(dictDao)
    val dao =
      new TSDaoHBase(schema, connection, namespace, dictProvider, tsdbConfig.putBatchSize, tsdbConfig.reduceLimit)
    val changelogDao = new ChangelogDaoHBase(connection, namespace)

    new TSDB(schema, dao, changelogDao, prepareQuery, tsdbConfig, metricCollectorCreator)
  }

  def apply(
      config: Configuration,
      namespace: String,
      schema: Schema,
      prepareQuery: Query => Query,
      settings: Settings,
      tsdbConfig: TsdbConfig,
      metricCollectorCreator: Option[(Query, String) => MetricQueryCollector]
  ): TSDB = {
    val connection = ConnectionFactory.createConnection(config)
    HBaseUtils.initStorage(connection, namespace, schema, tsdbConfig)
    val metricsCollectorOrDefault =
      metricCollectorCreator.getOrElse(createDefaultMetricCollector(tsdbConfig, connection, namespace))
    TSDBHBase(connection, namespace, schema, prepareQuery, settings, tsdbConfig)(metricsCollectorOrDefault)
  }
}
