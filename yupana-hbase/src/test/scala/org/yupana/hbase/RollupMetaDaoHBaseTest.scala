package org.yupana.hbase

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.joda.time.{ DateTime, Interval }
import org.scalatest.{ FlatSpecLike, GivenWhenThen, Matchers }
import org.yupana.core.model.UpdateInterval
import org.yupana.hbase.HBaseUtilsTest.TestTable

trait RollupMetaDaoHBaseTest extends HBaseTestBase with FlatSpecLike with Matchers with GivenWhenThen {

  private lazy val hbaseConnection = ConnectionFactory.createConnection(getConfiguration)

  "RollupMetaDaoHBase" should "handle invalid periods" in {
    val dao = new RollupMetaDaoHBase(hbaseConnection, "test")

    val baseTimes = Set(
      HBaseUtils.baseTime(DateTime.now().withDayOfMonth(3).getMillis, TestTable),
      HBaseUtils.baseTime(DateTime.now().withDayOfMonth(4).getMillis, TestTable)
    )

    val invalidatedIntervals = baseTimes.map { baseTime =>
      UpdateInterval(from = baseTime, to = baseTime + TestTable.rowTimeSpan)
    }.toSeq

    val from = DateTime.now().plusDays(-1).getMillis
    val to = DateTime.now().plusDays(1).getMillis
    val interval = new Interval(from, to)

    When("invalid baseTimes was put")
    dao.putUpdatesIntervals("receipt", invalidatedIntervals)

    Then("returned periods must be empty")
    dao.getUpdatesIntervals("rollup_by_day", interval) should have size 0

    And("invalid periods must be non empty")
    dao.getUpdatesIntervals("receipt", interval) should have size 2
    dao.getUpdatesIntervals("rollup_by_day", interval) should have size 0

    When("periods marks as recalculated")
    dao.putUpdatesIntervals("rollup_by_day", invalidatedIntervals)

    Then("rollup_by_day periods now exists")
    val result = dao.getUpdatesIntervals("rollup_by_day", interval)
    result should have size 2
    val period = result.head
    val t = baseTimes.head
    period.from shouldEqual t
    period.to shouldEqual t + TestTable.rowTimeSpan

    And("invalid periods still here")
    dao.getUpdatesIntervals("receipt", interval) should have size 2

    And("no new invalid periods")
    dao.getUpdatesIntervals("receipt", new Interval(to, DateTime.now().plusDays(2).getMillis)) should have size 0

    And("no new recalculated periods")
    dao.getUpdatesIntervals("rollup_by_day", new Interval(to, DateTime.now().plusDays(2).getMillis)) should have size 0
  }

}
