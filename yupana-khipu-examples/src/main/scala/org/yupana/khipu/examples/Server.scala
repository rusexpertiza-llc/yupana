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
import org.yupana.core.auth.{ DaoAuthorizer, PermissionService, TsdbRole, UserManager, YupanaUser }
import org.yupana.core.dao.UserDao
import org.yupana.core.providers.JdbcMetadataProvider
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.utils.metric.{ MetricQueryCollector, StandaloneMetricCollector }
import org.yupana.core.{ FlatQueryEngine, QueryEngineRouter, SimpleTsdbConfig }
import org.yupana.khipu.TSDBKhipu
import org.yupana.metrics.Slf4jMetricReporter
import org.yupana.netty.{ ServerContext, YupanaServer }

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

    val metricCreator = { (query: Query, user: String) =>
      new StandaloneMetricCollector(
        query,
        user,
        "query",
        tsdbConfig.metricsUpdateInterval,
        metricReporter
      )
    }
    val userDao = new UserDao {

      override def createUser(userName: String, password: Option[String], role: TsdbRole): Boolean = false

      override def updateUser(userName: String, password: Option[String], role: Option[TsdbRole]): Boolean = false

      override def deleteUser(userName: String): Boolean = false

      override def findUser(userName: String): Option[YupanaUser] = None

      override def listUsers(): List[YupanaUser] = List.empty
    }

    val tsdb = TSDBKhipu(schema, identity, tsdbConfig, config.settings, changelogDao, metricCreator)
    val userManager = new UserManager(userDao, Some("admin"), Some("admin"))

    val queryEngineRouter = new QueryEngineRouter(
      tsdb,
      new FlatQueryEngine(metricsDao, changelogDao),
      new JdbcMetadataProvider(schema, 2, 0, "2.0"),
      new SqlQueryProcessor(schema),
      new PermissionService(putEnabled = true),
      userManager
    )
    logger.info("Registering catalogs")

    val ctx = ServerContext(queryEngineRouter, new DaoAuthorizer(userManager))
    val server = new YupanaServer(config.host, config.port, 4, ctx)
    val f = server.start()
    logger.info(s"Yupana server started, listening on ${config.host}:${config.port}")
    Await.ready(f, Duration.Inf)
  }
}
