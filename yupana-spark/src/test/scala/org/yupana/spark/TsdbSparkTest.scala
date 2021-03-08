package org.yupana.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.hadoop.hbase.{ HBaseTestingUtility, StartMiniClusterOption }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.Query
import org.yupana.api.schema.ExternalLink
import org.yupana.core.ExternalLinkService
import org.yupana.schema.{ Dimensions, ItemTableMetrics, SchemaRegistry, Tables }

import java.nio.file.Paths

class TsdbSparkTest extends AnyFlatSpec with Matchers with DataFrameSuiteBase {

  private val utility = new HBaseTestingUtility

  override def beforeAll(): Unit = {
    super.beforeAll()
    val hadoopHomePath =
      Paths.get(ClassLoader.getSystemResource("hadoop").toURI)

    println(s"hhp $hadoopHomePath")

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

  import org.yupana.api.query.syntax.All._

  "TsdbSpark" should "run queries" in {

    val zkPort = utility.getZkCluster.getClientPort
    val config = new Config(conf.set("hbase.zookeeper", s"localhost:$zkPort"))

    println(config)

    val tsdbSpark = new TsdbSparkBase(sc, identity, config, SchemaRegistry.defaultSchema) {
      override def registerExternalLink(
          catalog: ExternalLink,
          catalogService: ExternalLinkService[_ <: ExternalLink]
      ): Unit = {}
      override def linkService(catalog: ExternalLink): ExternalLinkService[_ <: ExternalLink] = ???
    }

    val query = Query(
      Tables.itemsKkmTable,
      const(Time(1234567)),
      const(Time(2345678)),
      Seq(
        truncDay(time) as "day",
        min(divFrac(metric(ItemTableMetrics.sumField), double2bigDecimal(metric(ItemTableMetrics.quantityField)))) as "min_price",
        dimension(Dimensions.ITEM).toField
      ),
      None,
      Seq(truncDay(time), dimension(Dimensions.ITEM))
    )

    val result = tsdbSpark.query(query).collect()

    result(0).get[Int]("day") shouldEqual 2000000
  }
}
