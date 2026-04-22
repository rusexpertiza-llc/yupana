package org.yupana.hbase

import com.dimafeng.testcontainers.{ ForAllTestContainer, GenericContainer }
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.testcontainers.containers.wait.strategy.Wait

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

  val container: GenericContainer = {
    logger.info("instantiating Hbase Container " + ImageName)
    val gc = new GenericContainer(
      ImageName,
      Seq(2181, 16000, 16010, 16020),
      waitStrategy = Some(Wait.forHttp("/").forPort(16010).forStatusCode(200))
    )
//    gc.container.withNetworkMode("host")
    gc
  }

  override def getConfiguration: Configuration = {
    val hBaseConfiguration = HBaseConfiguration.create()
    println(s"!!!! ${container.host}")
    hBaseConfiguration.set("hbase.zookeeper.quorum", container.host)
    hBaseConfiguration.get("hbase.zookeeper.property.clientPort", container.firstMappedPort.toString)
    hBaseConfiguration
  }

  override lazy val connection = new ExternalLinkHBaseConnection(getConfiguration, "test")
}
