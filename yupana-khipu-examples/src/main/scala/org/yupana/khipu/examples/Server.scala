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

package org.yupana.khipu.examples

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.yupana.api.query.Query
import org.yupana.core.providers.JdbcMetadataProvider
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.utils.metric.{ MetricQueryCollector, StandaloneMetricCollector }
import org.yupana.core.{ FlatQueryEngine, QueryEngineRouter, SimpleTsdbConfig, TimeSeriesQueryEngine }
import org.yupana.khipu.TSDBKhipu
import org.yupana.metrics.Slf4jMetricReporter
import org.yupana.netty.{ NonEmptyUserAuthorizer, ServerContext, YupanaServer }

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Server extends StrictLogging {

  def main(args: Array[String]): Unit = {

    val schema = ExampleSchema.schema

    val config = Config.create(ConfigFactory.load())
    val tsdbConfig = SimpleTsdbConfig(collectMetrics = true, putEnabled = true)

    val metricsDao = new DummyMetricsDao
    val changelogDao = new DummyChangelogDao

    val metricReporter = new Slf4jMetricReporter[MetricQueryCollector]

    val mc = (query: Query) => new StandaloneMetricCollector(query, "select", 1000, metricReporter)

    val tsdb = TSDBKhipu(schema, identity, tsdbConfig, config.settings, changelogDao, mc)

    val queryEngineRouter = new QueryEngineRouter(
      new TimeSeriesQueryEngine(tsdb),
      new FlatQueryEngine(metricsDao, changelogDao),
      new JdbcMetadataProvider(schema, 2, 0, "2.0"),
      new SqlQueryProcessor(schema)
    )
    logger.info("Registering catalogs")

    val ctx = ServerContext(queryEngineRouter, NonEmptyUserAuthorizer)
    val server = new YupanaServer(config.host, config.port, 4, ctx)
    val f = server.start()
    logger.info(s"Yupana server started, listening on ${config.host}:${config.port}")
    Await.ready(f, Duration.Inf)
  }
}
