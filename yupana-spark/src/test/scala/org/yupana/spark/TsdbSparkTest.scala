package org.yupana.spark

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.{ DataPoint, Query }
import org.yupana.api.schema.{ ExternalLink, MetricValue }
import org.yupana.core.ExternalLinkService
import org.yupana.core.auth.YupanaUser
import org.yupana.core.dao.ChangelogDao
import org.yupana.hbase.ChangelogDaoHBase
import org.yupana.schema.{ Dimensions, ItemTableMetrics, SchemaRegistry, Tables }

import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit
import scala.collection.mutable

trait TsdbSparkTest extends AnyFlatSpecLike with Matchers with SharedSparkSession with SparkTestEnv {

  import org.yupana.api.query.syntax.All._

  val testUser = YupanaUser("test")

  "TsdbSpark" should "run queries" in {

    val config =
      new Config(
        sc.getConf
          .set("hbase.zookeeper", s"localhost:$getZkPort")
          .set("tsdb.hbase.compression", "none")
      )

    val tsdbSpark = new TsdbSparkBase(sc, identity, config, SchemaRegistry.defaultSchema)() {
      override def registerExternalLink(
          catalog: ExternalLink,
          catalogService: ExternalLinkService[_ <: ExternalLink]
      ): Unit = {}
      override def linkService(catalog: ExternalLink): ExternalLinkService[_ <: ExternalLink] = ???

      @transient
      override val changelogDao: ChangelogDao = new ChangelogDaoHBase(
        ConnectionFactory.createConnection(TsDaoHBaseSpark.hbaseConfiguration(config)),
        config.hbaseNamespace
      )

      override def externalLinkServices: Iterable[ExternalLinkService[_]] = Nil
    }

    val now = OffsetDateTime.now()

    val dps = sc.parallelize(
      Seq(
        DataPoint(
          Tables.itemsKkmTable,
          now.minusDays(3).toInstant.toEpochMilli,
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
          now.toInstant.toEpochMilli,
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
          now.plusDays(3).toInstant.toEpochMilli,
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

    tsdbSpark.put(dps, testUser)

    val query = Query(
      Tables.itemsKkmTable,
      const(Time(now.minusDays(1))),
      const(Time(now.plusDays(1))),
      Seq(
        truncDay(time) as "day",
        min(
          divFrac(metric(ItemTableMetrics.sumField), double2bigDecimal(metric(ItemTableMetrics.quantityField)))
        ) as "min_price",
        dimension(Dimensions.KKM_ID).toField
      ),
      None,
      Seq(truncDay(time), dimension(Dimensions.ITEM))
    )

    val result = tsdbSpark.query(query).collect()

    result should have size 1
    result(0)
      .get[Time]("day")
      .toLocalDateTime shouldEqual now.truncatedTo(ChronoUnit.DAYS).toLocalDateTime
    // withZone(DateTimeZone.UTC).withTimeAtStartOfDay().toLocalDateTime

    result(0).get[Int]("kkmId") shouldEqual 123

    result(0).get[BigDecimal]("min_price") shouldEqual BigDecimal(48)

  }

  "Etl" should "put data" in {
    import ETLFunctions._

    val ssc = new StreamingContext(sc, Seconds(1))
    val ecfg = new EtlConfig(sc.getConf)
    val ec = new EtlContext(ecfg, SchemaRegistry.defaultSchema)

    val queue = new mutable.Queue[RDD[DataPoint]]
    val stream = ssc.queueStream(queue)

    ssc.start()
    stream.saveDataPoints(ec)

    ssc.awaitTermination()
  }
}
