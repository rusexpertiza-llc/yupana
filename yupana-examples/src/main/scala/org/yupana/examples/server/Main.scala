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

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ ConnectionFactory, HBaseAdmin }
import org.yupana.api.query.Query
import org.yupana.core.auth.{ DaoAuthorizer, PermissionService, UserManager }
import org.yupana.core.providers.JdbcMetadataProvider
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.utils.metric.{ PersistentMetricQueryReporter, StandaloneMetricCollector }
import org.yupana.core.{ FlatQueryEngine, QueryEngineRouter, SimpleTsdbConfig }
import org.yupana.examples.ExampleSchema
import org.yupana.examples.externallinks.ExternalLinkRegistrator
import org.yupana.externallinks.universal.{ JsonCatalogs, JsonExternalLinkDeclarationsParser }
import org.yupana.hbase._
import org.yupana.metrics.{ CombinedMetricReporter, Slf4jMetricReporter }
import org.yupana.netty.{ ServerContext, YupanaServer }

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main extends StrictLogging {

  def main(args: Array[String]): Unit = {
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

    val tsdbConfig = SimpleTsdbConfig(collectMetrics = true, putEnabled = true)
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

    val metricCreator = { (query: Query, user: String) =>
      new StandaloneMetricCollector(
        query,
        user,
        "query",
        tsdbConfig.metricsUpdateInterval,
        metricReporter
      )
    }

    val tsdb =
      TSDBHBase(connection, config.hbaseNamespace, schemaWithJson, identity, config.settings, tsdbConfig)(
        metricCreator
      )

    val userDao = new UserDaoHBase(connection, config.hbaseNamespace)
    val userManager = new UserManager(userDao, Some("admin"), Some("admin"))

    val queryEngineRouter = new QueryEngineRouter(
      tsdb,
      new FlatQueryEngine(metricsDao, changelogDao),
      new JdbcMetadataProvider(schemaWithJson, 2, 0, "2.0"),
      new SqlQueryProcessor(schemaWithJson),
      new PermissionService(putEnabled = true),
      userManager
    )
    logger.info("Registering catalogs")
    val elRegistrator =
      new ExternalLinkRegistrator(tsdb, hbaseConfiguration, config.hbaseNamespace, config.settings)
    elRegistrator.registerAll(schemaWithJson)
    logger.info("Registering catalogs done")

    val ctx = ServerContext(queryEngineRouter, new DaoAuthorizer(userManager))
    val server = new YupanaServer(config.host, config.port, 4, ctx)
    val f = server.start()
    logger.info(s"Yupana server started, listening on ${config.host}:${config.port}")
    Await.ready(f, Duration.Inf)
  }
}
