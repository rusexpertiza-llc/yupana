package org.yupana.khipu.examples

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.pekko.actor.ActorSystem
import org.yupana.api.query.Query
import org.yupana.core.providers.JdbcMetadataProvider
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.utils.metric.{ MetricQueryCollector, StandaloneMetricCollector }
import org.yupana.core.{ FlatQueryEngine, QueryEngineRouter, SimpleTsdbConfig, TimeSeriesQueryEngine }
import org.yupana.khipu.TSDBKhipu
import org.yupana.metrics.Slf4jMetricReporter
import org.yupana.pekko.{ RequestHandler, TsdbTcp }

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Server extends StrictLogging {

  def main(args: Array[String]): Unit = {

    implicit val actorSystem: ActorSystem = ActorSystem("Yupana")

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
      new JdbcMetadataProvider(schema),
      new SqlQueryProcessor(schema)
    )
    logger.info("Registering catalogs")

    val requestHandler = new RequestHandler(queryEngineRouter)

    new TsdbTcp(requestHandler, config.host, config.port, 1, 0, "1.0")
    logger.info(s"Yupana server started, listening on ${config.host}:${config.port}")

    Await.ready(actorSystem.whenTerminated, Duration.Inf)

  }
}
