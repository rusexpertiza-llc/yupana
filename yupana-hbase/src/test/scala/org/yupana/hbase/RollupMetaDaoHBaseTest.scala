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

    When("invalid baseTimes was put")
    dao.putUpdatesIntervals("receipt", baseTimes.map { baseTime =>
      UpdateInterval(from = baseTime, to = baseTime + TestTable.rowTimeSpan, rollupTime = None)
    }.toSeq)

    Then("returned periods must be empty")
    val from = DateTime.now().plusDays(-1).getMillis
    val to = DateTime.now().plusDays(1).getMillis
    val interval = Some(new Interval(from, to))
    dao.getUpdatesIntervals("rollup_by_day", invalidated = false, interval) should have size 0
    dao.getUpdatesIntervals("rollup_by_day", invalidated = true, interval) should have size 0

    Then("and invalid periods must be non empty")
    val invalidatedPeriods = dao.getUpdatesIntervals("receipt", invalidated = true).toSeq
    invalidatedPeriods should have size 2
    dao.getUpdatesIntervals("receipt", invalidated = false) should have size 0

    When("baseTimes marks as valid")
    val valid = invalidatedPeriods.map(_.copy(rollupTime = Some(DateTime.now().getMillis)))
    dao.putUpdatesIntervals("receipt", valid)
    val recalculated = invalidatedPeriods.map(_.copy(id = None, rollupTime = Some(DateTime.now().getMillis)))
    dao.putUpdatesIntervals("rollup_by_day", recalculated)

    Then("returned periods must be correct")
    val result = dao.getUpdatesIntervals("rollup_by_day", invalidated = false, interval)
    result should have size 2
    val period = result.head
    val t = baseTimes.head
    period.from shouldEqual t
    period.to shouldEqual t + TestTable.rowTimeSpan

    Then("and 2 periods are recalculated now")
    dao.getUpdatesIntervals("receipt", invalidated = false) should have size 2

    Then("and 0 periods to recalculate")
    dao.getUpdatesIntervals("receipt", invalidated = true) should have size 0
  }

}
