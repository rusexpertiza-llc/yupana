package org.yupana.spark

import com.dimafeng.testcontainers.{ Container, ForAllTestContainer, GenericContainer }
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.flatspec.AnyFlatSpec

trait SparkTestEnv {
  def getZkPort: Int
}

class SparkTestSuite
    extends AnyFlatSpec
    with TsdbSparkTest
    with DataRowRDDTest
    with ForAllTestContainer
    with StrictLogging {

  val ImageName = "pikkvile/hbase-2.4.15-standalone:1.0.0"

  override val container: Container = {
    logger.info("Instantiating HBase Container " + ImageName)
    val gc = new GenericContainer(ImageName)
    gc.container.withNetworkMode("host")
    gc
  }

  override def getZkPort: Int = 2181
}
