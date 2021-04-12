package org.yupana.spark

import org.apache.hadoop.hbase.{ HBaseTestingUtility, StartMiniClusterOption }
import org.joda.time.{ DateTime, DateTimeZone }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.{ DataPoint, Query }
import org.yupana.api.schema.{ ExternalLink, MetricValue }
import org.yupana.core.ExternalLinkService
import org.yupana.schema.{ Dimensions, ItemTableMetrics, SchemaRegistry, Tables }

class TsdbSparkTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll with SharedSparkSession {

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

  import org.yupana.api.query.syntax.All._

  "TsdbSpark" should "run queries" in {

    val zkPort = utility.getZkCluster.getClientPort
    val config =
      new Config(
        sc.getConf
          .set("hbase.zookeeper", s"localhost:$zkPort")
          .set("tsdb.hbase.compression", "none")
      )

    val tsdbSpark = new TsdbSparkBase(sc, identity, config, SchemaRegistry.defaultSchema) {
      override def registerExternalLink(
          catalog: ExternalLink,
          catalogService: ExternalLinkService[_ <: ExternalLink]
      ): Unit = {}
      override def linkService(catalog: ExternalLink): ExternalLinkService[_ <: ExternalLink] = ???
    }

    val now = DateTime.now()

    val dps = sc.parallelize(
      Seq(
        DataPoint(
          Tables.itemsKkmTable,
          now.minusDays(3).getMillis,
          Map(
            Dimensions.ITEM -> "сигареты",
            Dimensions.KKM_ID -> 123,
            Dimensions.OPERATION_TYPE -> 1.toByte,
            Dimensions.POSITION -> 1.toShort
          ),
          Seq(MetricValue(ItemTableMetrics.sumField, BigDecimal(123)), MetricValue(ItemTableMetrics.quantityField, 10d))
        ),
        DataPoint(
          Tables.itemsKkmTable,
          now.getMillis,
          Map(
            Dimensions.ITEM -> "телевизор",
            Dimensions.KKM_ID -> 123,
            Dimensions.OPERATION_TYPE -> 1.toByte,
            Dimensions.POSITION -> 1.toShort
          ),
          Seq(MetricValue(ItemTableMetrics.sumField, BigDecimal(240)), MetricValue(ItemTableMetrics.quantityField, 5d))
        ),
        DataPoint(
          Tables.itemsKkmTable,
          now.plusDays(3).getMillis,
          Map(
            Dimensions.ITEM -> "сосиски",
            Dimensions.KKM_ID -> 123,
            Dimensions.OPERATION_TYPE -> 1.toByte,
            Dimensions.POSITION -> 1.toShort
          ),
          Seq(MetricValue(ItemTableMetrics.sumField, BigDecimal(643)), MetricValue(ItemTableMetrics.quantityField, 1d))
        )
      )
    )

    tsdbSpark.writeRDD(dps, Tables.itemsKkmTable)

    val query = Query(
      Tables.itemsKkmTable,
      const(Time(now.minusDays(1))),
      const(Time(now.plusDays(1))),
      Seq(
        truncDay(time) as "day",
        min(divFrac(metric(ItemTableMetrics.sumField), double2bigDecimal(metric(ItemTableMetrics.quantityField)))) as "min_price",
        dimension(Dimensions.KKM_ID).toField
      ),
      None,
      Seq(truncDay(time), dimension(Dimensions.ITEM))
    )

    val result = tsdbSpark.query(query).collect()

    result should have size 1
    result(0)
      .get[Time]("day")
      .toLocalDateTime shouldEqual now.withZone(DateTimeZone.UTC).withTimeAtStartOfDay().toLocalDateTime

    result(0).get[Int]("kkmId") shouldEqual 123

    result(0).get[BigDecimal]("min_price") shouldEqual BigDecimal(48)

  }
}
