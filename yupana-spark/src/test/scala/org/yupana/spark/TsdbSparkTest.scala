package org.yupana.spark

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.yupana.api.{ Currency, Time }
import org.yupana.api.query.{ DataPoint, Query }
import org.yupana.api.schema.{ ExternalLink, MetricValue }
import org.yupana.core.ExternalLinkService
import org.yupana.core.auth.{ TsdbRole, YupanaUser }
import org.yupana.core.dao.ChangelogDao
import org.yupana.hbase.ChangelogDaoHBase
import org.yupana.schema.{ Dimensions, ItemTableMetrics, SchemaRegistry, Tables }

import java.sql.Timestamp
import java.time.{ LocalDateTime, OffsetDateTime, ZoneOffset }
import java.time.temporal.ChronoUnit

trait TsdbSparkTest extends AnyFlatSpecLike with Matchers with SharedSparkSession with SparkTestEnv {

  import org.yupana.api.query.syntax.All._

  val testUser = YupanaUser("test", None, TsdbRole.ReadWrite)

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

    val now = OffsetDateTime.now(ZoneOffset.UTC)

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
          Seq(MetricValue(ItemTableMetrics.sumField, Currency(12300)), MetricValue(ItemTableMetrics.quantityField, 10d))
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
          Seq(MetricValue(ItemTableMetrics.sumField, Currency(24000)), MetricValue(ItemTableMetrics.quantityField, 5d))
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
          Seq(MetricValue(ItemTableMetrics.sumField, Currency(64300)), MetricValue(ItemTableMetrics.quantityField, 1d))
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
          div(metric(ItemTableMetrics.sumField), metric(ItemTableMetrics.quantityField))
        ) as "min_price",
        dimension(Dimensions.KKM_ID).toField
      ),
      None,
      Seq(truncDay(time), dimension(Dimensions.KKM_ID))
    )

    val result = tsdbSpark.query(query).toSparkSqlRDD.collect()

    result should have size 1

    LocalDateTime.ofInstant(result(0).getAs[Timestamp]("day").toInstant, ZoneOffset.UTC) shouldEqual now
      .truncatedTo(ChronoUnit.DAYS)
      .toLocalDateTime
    // withZone(DateTimeZone.UTC).withTimeAtStartOfDay().toLocalDateTime

    result(0).getAs[Int]("kkmId") shouldEqual 123

    result(0).getAs[BigDecimal]("min_price") shouldEqual BigDecimal(48)

  }
}
