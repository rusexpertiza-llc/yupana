package org.yupana.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{ HBaseTestingUtility, StartMiniClusterOption }
import org.scalatest.BeforeAndAfterAll
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
    with BeforeAndAfterAll {
  private val utility = new HBaseTestingUtility

  override def getConfiguration: Configuration = utility.getConfiguration

  override val connection = new ExternalLinkHBaseConnection(getConfiguration, "test")

  override def beforeAll(): Unit = {
    utility.startMiniCluster(
      StartMiniClusterOption
        .builder()
        .numMasters(1)
        .numRegionServers(1)
        .numDataNodes(1)
        .build()
    )
  }

  override protected def afterAll(): Unit = {
    utility.shutdownMiniCluster()
  }
}
