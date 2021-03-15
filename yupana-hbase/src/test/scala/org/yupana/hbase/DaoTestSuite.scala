package org.yupana.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseTestingUtility
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
    with RollupMetaDaoHBaseTest
    with BeforeAndAfterAll {
  private val utility = new HBaseTestingUtility

  override def getConfiguration: Configuration = utility.getConfiguration

  override val connection = new ExternalLinkHBaseConnection(getConfiguration, "test")

  override def beforeAll(): Unit = {
    utility.startMiniCluster(1, 1)
  }

  override protected def afterAll(): Unit = {
    utility.shutdownMiniCluster()
  }
}
