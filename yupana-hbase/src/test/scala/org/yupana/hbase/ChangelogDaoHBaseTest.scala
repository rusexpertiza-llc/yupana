package org.yupana.hbase

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.scalatest.GivenWhenThen
import org.yupana.core.model.UpdateInterval
import org.yupana.hbase.HBaseUtilsTest.TestTable
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.time.{ Instant, OffsetDateTime, ZoneOffset }
import java.time.temporal.ChronoUnit

trait ChangelogDaoHBaseTest extends HBaseTestBase with AnyFlatSpecLike with Matchers with GivenWhenThen {

  private lazy val hbaseConnection = ConnectionFactory.createConnection(getConfiguration)

  "RollupMetaDaoHBase" should "handle invalid periods" in {
    val dao = new ChangelogDaoHBase(hbaseConnection, "test")

    val baseTimes = Set(
      HBaseUtils.baseTime(OffsetDateTime.now().minusDays(4).toInstant.toEpochMilli, TestTable),
      HBaseUtils.baseTime(OffsetDateTime.now().minusDays(3).toInstant.toEpochMilli, TestTable)
    )

    val now = OffsetDateTime.now()

    def invalidatedIntervals(table: String) =
      baseTimes.map { baseTime =>
        UpdateInterval(
          table,
          from = OffsetDateTime.ofInstant(Instant.ofEpochMilli(baseTime), ZoneOffset.UTC),
          to = OffsetDateTime.ofInstant(Instant.ofEpochMilli(baseTime + TestTable.rowTimeSpan), ZoneOffset.UTC),
          now,
          "test"
        )
      }.toSeq

    val from = now.minusDays(1).toInstant.toEpochMilli
    val to = now.plusDays(1).toInstant.toEpochMilli

    When("invalid baseTimes was put")
    dao.putUpdatesIntervals(invalidatedIntervals("receipt"))

    Then("returned periods must be empty")
    dao.getUpdatesIntervals(Some("rollup_by_day"), Some(from), Some(to)) should have size 0

    And("invalid periods must be non empty")
    dao.getUpdatesIntervals(Some("receipt"), Some(from), Some(to)) should have size 2
    dao.getUpdatesIntervals(Some("rollup_by_day"), Some(from), Some(to)) should have size 0

    When("periods marks as recalculated")
    dao.putUpdatesIntervals(invalidatedIntervals("rollup_by_day"))

    Then("rollup_by_day periods now exists")
    val result = dao.getUpdatesIntervals(Some("rollup_by_day"), Some(from), Some(to))
    result should have size 2
    val period = result.head
    val t = OffsetDateTime.ofInstant(Instant.ofEpochMilli(baseTimes.head), ZoneOffset.UTC)
    period.from shouldEqual t
    period.to shouldEqual t.plus(TestTable.rowTimeSpan.toInt, ChronoUnit.MILLIS)

    And("invalid periods still here")
    dao.getUpdatesIntervals(Some("receipt"), Some(from), Some(to)) should have size 2

    And("no new invalid periods")
    dao.getUpdatesIntervals(Some("receipt"), Some(to), Some(OffsetDateTime.now().plusDays(2).toInstant.toEpochMilli)) should have size 0

    And("no new recalculated periods")
    dao.getUpdatesIntervals(
      Some("rollup_by_day"),
      Some(to),
      Some(OffsetDateTime.now().plusDays(2).toInstant.toEpochMilli)
    ) should have size 0
  }

}
