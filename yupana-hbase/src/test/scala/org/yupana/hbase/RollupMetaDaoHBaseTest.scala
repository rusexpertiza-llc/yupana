package org.yupana.hbase

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.joda.time.{ DateTime, LocalDateTime }
import org.scalatest.{ FlatSpecLike, GivenWhenThen, Matchers }
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
    dao.putInvalidatedBaseTimes(baseTimes, TestTable.rowTimeSpan)

    Then("returned periods must be empty")
    val from = LocalDateTime.now().plusDays(-1)
    val to = LocalDateTime.now().plusDays(1)
    dao.getRecalculatedPeriods(from, to) should have size 0

    Then("and invalid periods must be non empty")
    val invalid = dao.getInvalidatedBaseTimes
    invalid should contain theSameElementsAs baseTimes

    When("baseTimes marks as valid")
    dao.markBaseTimesRecalculated(baseTimes)

    Then("returned periods must be correct")
    val result = dao.getRecalculatedPeriods(from, to)
    result should have size 2
    val period = result.head
    val t = baseTimes.head
    period.from shouldEqual t
    period.to shouldEqual t + TestTable.rowTimeSpan

    Then("and invalid periods must be empty")
    dao.getInvalidatedBaseTimes should have size 0
  }

}
