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
    val invalidatedPeriods = baseTimes.map { baseTime =>
      UpdateInterval(baseTime, baseTime + TestTable.rowTimeSpan, None)
    }.toSeq
    When("invalid baseTimes was put")
    dao.putUpdatesIntervals(invalidatedPeriods)

    Then("returned periods must be empty")
    val from = DateTime.now().plusDays(-1).getMillis
    val to = DateTime.now().plusDays(1).getMillis
    val interval = Some(new Interval(from, to))
    dao.getUpdatesIntervals(interval) should have size 0

    Then("and invalid periods must be non empty")
    dao.getUpdatesIntervals() should contain theSameElementsAs invalidatedPeriods

    When("baseTimes marks as valid")
    val valid = invalidatedPeriods.map(_.copy(rollupTime = Some(DateTime.now().getMillis)))
    dao.putUpdatesIntervals(valid)

    Then("returned periods must be correct")
    val result = dao.getUpdatesIntervals(interval)
    result should have size 2
    val period = result.head
    val t = baseTimes.head
    period.from shouldEqual t
    period.to shouldEqual t + TestTable.rowTimeSpan

    Then("and invalid periods must be empty")
    dao.getUpdatesIntervals() should have size 0
  }

}
