package org.yupana.hbase

import com.dimafeng.testcontainers.{ FixedHostPortGenericContainer, ForAllTestContainer }
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable

import scala.io.Source

trait HBaseTestBase {
  def getConfiguration: Configuration
  def connection: ExternalLinkHBaseConnection
}

class DaoTestSuite
    extends AnyFlatSpec
    with BTreeIndexDaoHBaseTest
    with InvertedIndexDaoHBaseTest
    with DictionaryDaoHBaseTest
    with TsdbQueryMetricsDaoHBaseTest
    with ChangelogDaoHBaseTest
    with TsdbHBaseTest
    with ForAllTestContainer
    with StrictLogging {

  val ImageName = "pikkvile/hbase-2.4.15-standalone:1.0.0"

  val container: FixedHostPortGenericContainer = {
    logger.info("instantiating Hbase Container " + ImageName)
    val hbasePorts = Seq(2181, 16000, 16010, 16020)
    val gc = new FixedHostPortGenericContainer(
      ImageName,
      exposedPorts = hbasePorts,
      portBindings = hbasePorts.map(x => (x, x)),
      waitStrategy = Some(Wait.forHttp("/").forPort(16010).forStatusCode(200))
    )
    gc.container.withCopyToContainer(
      Transferable.of(Source.fromResource("hbase-site.xml").getLines().mkString),
      "/opt/hbase/conf/hbase-site.xml"
    )
//    gc.container.withFixedExposedPort(2121, 2121)
//    gc.container.withNetworkMode("host")
    gc
  }

  override def getConfiguration: Configuration = {
    val hBaseConfiguration = HBaseConfiguration.create()
    println(s"!!!! ${container.host}")
    hBaseConfiguration.set("hbase.zookeeper.quorum", container.host)
    hBaseConfiguration.get("hbase.zookeeper.property.clientPort", "2181")

    hBaseConfiguration.set("hbase.master.hostname", container.host)
    hBaseConfiguration.set("hbase.regionserver.hostname", container.host)

    hBaseConfiguration.set("hbase.client.retries.number", "3")
    hBaseConfiguration.set("hbase.client.pause", "1000")
    hBaseConfiguration.set("zookeeper.recovery.retry", "1")

    hBaseConfiguration
  }

  override lazy val connection = new ExternalLinkHBaseConnection(getConfiguration, "test")
}
