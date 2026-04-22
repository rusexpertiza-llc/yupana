package org.yupana.spark

import com.dimafeng.testcontainers.{ FixedHostPortGenericContainer, ForAllTestContainer }
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.flatspec.AnyFlatSpec
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.Transferable

import scala.io.Source

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

  override def getZkPort: Int = container.container.getMappedPort(2181)
}
