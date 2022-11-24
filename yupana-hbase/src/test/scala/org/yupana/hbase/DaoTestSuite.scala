package org.yupana.hbase

import com.dimafeng.testcontainers.{ ForAllTestContainer, GenericContainer }
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.scalatest.flatspec.AnyFlatSpec

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
    with ForAllTestContainer
    with StrictLogging {

  val ImageName = "pikkvile/hbase-2.4.15-standalone:1.0.0"

  val container: GenericContainer = {
    logger.info("instantiating Hbase Container " + ImageName)
    val gc = new GenericContainer(ImageName)
    gc.container.withNetworkMode("host")
    gc
  }

  override def getConfiguration: Configuration = {
    val hBaseConfiguration = HBaseConfiguration.create()
    hBaseConfiguration.set("hbase.zookeeper.quorum", "localhost")
    hBaseConfiguration.get("hbase.zookeeper.property.clientPort", "2181")
    hBaseConfiguration
  }

  override val connection = new ExternalLinkHBaseConnection(getConfiguration, "test")
}
