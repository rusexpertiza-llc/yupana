package org.yupana.spark

import org.apache.hadoop.hbase.{ HBaseTestingUtility, StartMiniClusterOption }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

trait SparkTestEnv {
  def getZkPort: Int
}

class SparkTestSuite extends AnyFlatSpec with BeforeAndAfterAll with TsdbSparkTest with DataRowRDDTest {

  private val utility = new HBaseTestingUtility

  override def beforeAll(): Unit = {
    super.beforeAll()

    utility.startMiniCluster(
      StartMiniClusterOption
        .builder()
        .numMasters(1)
        .numRegionServers(1)
        .numDataNodes(1)
        .build()
    )
  }

  override def afterAll(): Unit = {
    utility.shutdownMiniCluster()
    super.afterAll()
  }

  override def getZkPort: Int = utility.getZkCluster.getClientPort
}
