package org.yupana.examples.server

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.yupana.akka.{RequestHandler, TsdbTcp}
import org.yupana.examples.ExampleSchema
import org.yupana.examples.externallinks.ExternalLinkRegistrator
import org.yupana.externallinks.universal.{JsonCatalogs, JsonExternalLinkDeclarationsParser}
import org.yupana.hbase.{HdfsFileUtils, TSDBHbase}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main extends StrictLogging {

  def main(args: Array[String]): Unit = {

    implicit val actorSystem: ActorSystem = ActorSystem("Yupana")

    val config = Config.create(ConfigFactory.load())

    val hbaseConfiguration = HBaseConfiguration.create()
    hbaseConfiguration.set("hbase.zookeeper.quorum", config.hbaseZookeeperUrl)
    hbaseConfiguration.set("zookeeper.session.timeout", "180000")
    hbaseConfiguration.set("hbase.client.scanner.timeout.period", "180000")
    HdfsFileUtils.addHdfsPathToConfiguration(hbaseConfiguration, config.properties)

    HBaseAdmin.checkHBaseAvailable(hbaseConfiguration)
    logger.info("TSDB HBase Configuration: {} works fine", hbaseConfiguration)

    val schema = ExampleSchema.schema
    val jsonLinks = Option(config.properties.getProperty("yupana.json-catalogs-declaration"))
    val schemaWithJson = jsonLinks.map(json =>
        JsonExternalLinkDeclarationsParser.parse(schema, json)
          .right.map(configs => JsonCatalogs.attachLinksToSchema(schema, configs))
      ).getOrElse(Right(schema))
      .fold(msg => throw new RuntimeException(s"Cannot register JSON catalogs: $msg"), identity)

    val tsdb = TSDBHbase(hbaseConfiguration, config.hbaseNamespace, schemaWithJson, identity, config.properties)
    logger.info("Registering catalogs")
    val elRegistrator = new ExternalLinkRegistrator(tsdb, hbaseConfiguration, config.hbaseNamespace, config.properties)
    elRegistrator.registerAll(schemaWithJson)
    logger.info("Registering catalogs done")

    val requestHandler = new RequestHandler(schemaWithJson)
    val tsdbTcp = new TsdbTcp(tsdb, requestHandler, config.host, config.port, 1, 0, "1.0")
    logger.info(s"Yupana server started, listening on ${config.host}:${config.port}")

    Await.ready(actorSystem.whenTerminated, Duration.Inf)
  }
}
