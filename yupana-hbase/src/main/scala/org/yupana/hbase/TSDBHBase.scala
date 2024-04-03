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

import org.apache.hadoop.hbase.client.{ Connection, ConnectionFactory }
import org.yupana.api.query.Query
import org.yupana.api.schema.Schema
import org.yupana.cache.CacheFactory
import org.yupana.core.dao.DictionaryProviderImpl
import org.yupana.core.utils.metric.{ MetricQueryCollector, PersistentMetricQueryReporter, StandaloneMetricCollector }
import org.yupana.core.{ TSDB, TsdbConfig }

object TSDBHBase {

  private def createDefaultMetricCollector(
      tsdbConfig: TsdbConfig,
      connection: Connection,
      namespace: String
  ): Query => MetricQueryCollector = {
    lazy val tsdbQueryMetricsDaoHBase = new TsdbQueryMetricsDaoHBase(connection, namespace)
    lazy val reporter = new PersistentMetricQueryReporter(tsdbQueryMetricsDaoHBase)

    { query: Query =>
      new StandaloneMetricCollector(
        query,
        "query",
        tsdbConfig.metricsUpdateInterval,
        reporter
      )
    }
  }

  def apply(
      connection: Connection,
      cfg: TSDBHBaseConfig,
      schema: Schema,
      prepareQuery: Query => Query
  )(
      metricCollectorCreator: Query => MetricQueryCollector =
        createDefaultMetricCollector(cfg, connection, cfg.hbaseNamespace)
  ): TSDB = {

    CacheFactory.init(cfg.settings)

    val dictDao = new DictionaryDaoHBase(connection, cfg.hbaseNamespace)
    val dictProvider = new DictionaryProviderImpl(dictDao)
    val dao =
      new TSDaoHBase(schema, connection, cfg.hbaseNamespace, dictProvider, cfg.putBatchSize, cfg.reduceLimit)
    val changelogDao = new ChangelogDaoHBase(connection, cfg.hbaseNamespace)

    new TSDB(schema, dao, changelogDao, prepareQuery, cfg, metricCollectorCreator)
  }

  def apply(
      cfg: TSDBHBaseConfig,
      schema: Schema,
      prepareQuery: Query => Query,
      metricCollectorCreator: Option[Query => MetricQueryCollector]
  ): TSDB = {
    val connection = ConnectionFactory.createConnection(cfg.hBaseConfiguration)
    HBaseUtils.initStorage(connection, cfg.hbaseNamespace, schema, cfg)
    val metricsCollectorOrDefault =
      metricCollectorCreator.getOrElse(createDefaultMetricCollector(cfg, connection, cfg.hbaseNamespace))
    TSDBHBase(connection, cfg, schema, prepareQuery)(metricsCollectorOrDefault)
  }
}
