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

package org.yupana.examples.server

import org.apache.pekko.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ ConnectionFactory, HBaseAdmin }
import org.yupana.pekko.{ RequestHandler, TsdbTcp }
import org.yupana.api.query.Query
import org.yupana.core.utils.metric.{ PersistentMetricQueryReporter, StandaloneMetricCollector }
import org.yupana.core.providers.JdbcMetadataProvider
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.{ FlatQueryEngine, QueryEngineRouter, SimpleTsdbConfig, TimeSeriesQueryEngine }
import org.yupana.core.utils.metric.{ PersistentMetricQueryReporter, StandaloneMetricCollector }
import org.yupana.core.{ FlatQueryEngine, QueryEngineRouter }
import org.yupana.examples.ExampleSchema
import org.yupana.examples.externallinks.ExternalLinkRegistrator
import org.yupana.externallinks.universal.{ JsonCatalogs, JsonExternalLinkDeclarationsParser }
import org.yupana.hbase._
import org.yupana.metrics.{ CombinedMetricReporter, Slf4jMetricReporter }

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main extends StrictLogging {

  def main(args: Array[String]): Unit = {

    implicit val actorSystem: ActorSystem = ActorSystem("Yupana")

    val hbaseRegionsMax = "hbase.regions.initial.max"
    val config = Config.create(ConfigFactory.load())

    val hbaseConfiguration = HBaseConfiguration.create()
    hbaseConfiguration.set("hbase.zookeeper.quorum", config.hbaseZookeeperUrl)
    hbaseConfiguration.set("zookeeper.session.timeout", "180000")
    hbaseConfiguration.set("hbase.client.scanner.timeout.period", "180000")
    hbaseConfiguration.set(hbaseRegionsMax, config.settings(hbaseRegionsMax))
    HdfsFileUtils.addHdfsPathToConfiguration(hbaseConfiguration, config.settings)

    HBaseAdmin.available(hbaseConfiguration)
    logger.info("TSDB HBase Configuration: {} works fine", hbaseConfiguration)

    val schema = ExampleSchema.schema
    val jsonLinks = config.settings.opt[String]("yupana.json-catalogs-declaration")
    val schemaWithJson = jsonLinks
      .map(json =>
        JsonExternalLinkDeclarationsParser
          .parse(schema, json)
          .map(configs => JsonCatalogs.attachLinksToSchema(schema, configs))
      )
      .getOrElse(Right(schema))
      .fold(msg => throw new RuntimeException(s"Cannot register JSON catalogs: $msg"), identity)

    val tsdbConfig = SimpleTSDBHBaseConfig(
      collectMetrics = true,
      putEnabled = true,
      hbaseNamespace = config.hbaseNamespace,
      hbaseZookeeper = config.hbaseZookeeperUrl,
      settings = config.settings
    )
    val connection = ConnectionFactory.createConnection(hbaseConfiguration)

    val changelogDao = new ChangelogDaoHBase(connection, config.hbaseNamespace)
    val metricsDao = new TsdbQueryMetricsDaoHBase(connection, config.hbaseNamespace)
    HBaseUtils.initStorage(connection, config.hbaseNamespace, schema, tsdbConfig)

    logger.info("TsdbQueryMetricsDao initialization...")
    val hbaseConnection = ConnectionFactory.createConnection(hbaseConfiguration)
    val tsdbQueryMetricsDaoHBase = new TsdbQueryMetricsDaoHBase(hbaseConnection, config.hbaseNamespace)
    val metricReporter = new CombinedMetricReporter(
      new Slf4jMetricReporter,
      new PersistentMetricQueryReporter(tsdbQueryMetricsDaoHBase)
    )

    val metricCreator = { query: Query =>
      new StandaloneMetricCollector(
        query,
        "query",
        tsdbConfig.metricsUpdateInterval,
        metricReporter
      )
    }

    val tsdb = TSDBHBase(connection, tsdbConfig, schemaWithJson, identity[Query] _)(metricCreator)

    val queryEngineRouter = new QueryEngineRouter(
      new TimeSeriesQueryEngine(tsdb),
      new FlatQueryEngine(metricsDao, changelogDao),
      new JdbcMetadataProvider(schemaWithJson),
      new SqlQueryProcessor(schemaWithJson)
    )
    logger.info("Registering catalogs")
    val elRegistrator =
      new ExternalLinkRegistrator(tsdb, hbaseConfiguration, config.hbaseNamespace, config.settings)
    elRegistrator.registerAll(schemaWithJson)
    logger.info("Registering catalogs done")

    val requestHandler = new RequestHandler(queryEngineRouter)
    new TsdbTcp(requestHandler, config.host, config.port, 1, 0, "1.0")
    logger.info(s"Yupana server started, listening on ${config.host}:${config.port}")

    Await.ready(actorSystem.whenTerminated, Duration.Inf)
  }
}
