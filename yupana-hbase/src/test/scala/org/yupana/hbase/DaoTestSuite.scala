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

  val hbaseSiteXml: String = Source.fromResource("hbase-site.xml").getLines().mkString

  val container: FixedHostPortGenericContainer = {
    logger.info("instantiating Hbase Container " + ImageName)
    val gc = new FixedHostPortGenericContainer(ImageName)
    gc.container.withExposedPorts(2181, 16010)
    gc.container.withFixedExposedPort(16000, 16000)
    gc.container.withFixedExposedPort(16010, 16010)
    gc.container.withFixedExposedPort(16020, 16020)
    gc.container.withCopyToContainer(Transferable.of(hbaseSiteXml), "/opt/hbase/conf/hbase-site.xml")
    gc.container.waitingFor(Wait.forHttp("/").forPort(16010).forStatusCode(200))
    gc
  }

  override def getConfiguration: Configuration = {
    val hBaseConfiguration = HBaseConfiguration.create()
    hBaseConfiguration.set("hbase.zookeeper.quorum", container.container.getHost)
    hBaseConfiguration.set("hbase.zookeeper.property.clientPort", container.container.getMappedPort(2181).toString)
    hBaseConfiguration.set("hbase.master.hostname", "localhost")
    hBaseConfiguration.set("hbase.regionserver.hostname", "localhost")
    hBaseConfiguration.set("hbase.client.retries.number", "3")
    hBaseConfiguration.set("hbase.client.pause", "1000")
    hBaseConfiguration.set("zookeeper.recovery.retry", "1")
    hBaseConfiguration
  }

  override def connection = new ExternalLinkHBaseConnection(getConfiguration, "test")
}
