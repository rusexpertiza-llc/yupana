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
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.{ DictionaryProviderImpl, TsdbQueryMetricsDao }
import org.yupana.core.{ TSDB, TsdbConfig }

import java.util.Properties

object TSDBHBase {
  def apply(
      connection: Connection,
      namespace: String,
      schema: Schema,
      prepareQuery: Query => Query,
      properties: Properties,
      tsdbConfig: TsdbConfig,
      metricsDao: TsdbQueryMetricsDao
  ): TSDB = {

    CacheFactory.init(properties, namespace)

    val dictDao = new DictionaryDaoHBase(connection, namespace)
    val dictProvider = new DictionaryProviderImpl(dictDao)
    val dao = new TSDaoHBase(schema, connection, namespace, dictProvider, tsdbConfig.putBatchSize)
    new TSDB(schema, dao, metricsDao, dictProvider, prepareQuery, tsdbConfig)
  }

  def apply(
      config: Configuration,
      namespace: String,
      schema: Schema,
      prepareQuery: Query => Query,
      properties: Properties,
      tsdbConfig: TsdbConfig
  ): TSDB = {
    val connection = ConnectionFactory.createConnection(config)
    HBaseUtils.initStorage(connection, namespace, schema, tsdbConfig)
    val metricsDao = new TsdbQueryMetricsDaoHBase(connection, namespace)
    TSDBHBase(connection, namespace, schema, prepareQuery, properties, tsdbConfig, metricsDao)
  }
}
