package org.yupana.hbase

import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.{ DataPoint, Query }
import org.yupana.api.schema.MetricValue
import org.yupana.core.{ SimpleTsdbConfig, TestDims, TestSchema, TestTableFields }
import org.yupana.settings.Settings

import java.time.{ LocalDateTime, ZoneOffset }
import java.util.Properties

trait TsdbHBaseTest extends HBaseTestBase with AnyFlatSpecLike with Matchers {

  import org.yupana.api.query.syntax.All._

  "TsdbHBase" should "put and query data" in {

    val props = new Properties
    val tsdb =
      TSDBHBase(
        getConfiguration,
        "test",
        TestSchema.schema,
        identity,
        Settings(props),
        SimpleTsdbConfig(compression = "none", putEnabled = true),
        None
      )

    val now = LocalDateTime.now()

    val dps = Seq(
      DataPoint(
        TestSchema.testTable,
        now.minusDays(3).toInstant(ZoneOffset.UTC).toEpochMilli,
        Map(
          TestDims.DIM_A -> "сигареты",
          TestDims.DIM_B -> 123.toShort
        ),
        Seq(
          MetricValue(TestTableFields.TEST_BIGDECIMAL_FIELD, BigDecimal(123)),
          MetricValue(TestTableFields.TEST_FIELD, 10d)
        )
      ),
      DataPoint(
        TestSchema.testTable,
        now.toInstant(ZoneOffset.UTC).toEpochMilli,
        Map(
          TestDims.DIM_A -> "телевизор",
          TestDims.DIM_B -> 124.toShort
        ),
        Seq(
          MetricValue(TestTableFields.TEST_BIGDECIMAL_FIELD, BigDecimal(240)),
          MetricValue(TestTableFields.TEST_FIELD, 5d)
        )
      ),
      DataPoint(
        TestSchema.testTable,
        now.plusDays(3).toInstant(ZoneOffset.UTC).toEpochMilli,
        Map(
          TestDims.DIM_A -> "сосиски",
          TestDims.DIM_B -> 123.toShort
        ),
        Seq(
          MetricValue(TestTableFields.TEST_BIGDECIMAL_FIELD, BigDecimal(643)),
          MetricValue(TestTableFields.TEST_FIELD, 1d)
        )
      )
    )

    tsdb.put(dps.iterator)

    val result = tsdb.query(
      Query(
        TestSchema.testTable,
        const(Time(now.minusDays(5))),
        const(Time(now.plusDays(1))),
        Seq(
          dimension(TestDims.DIM_A) as "A",
          metric(TestTableFields.TEST_FIELD).toField,
          metric(TestTableFields.TEST_BIGDECIMAL_FIELD).toField
        ),
        equ(dimension(TestDims.DIM_B), const(123.toShort))
      )
    )

    result.hasNext shouldBe true

    val row = result.next()
    row.get[String]("A") shouldEqual "сигареты"

    result.hasNext shouldBe false
  }

}
