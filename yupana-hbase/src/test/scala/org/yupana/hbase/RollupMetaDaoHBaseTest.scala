package org.yupana.hbase

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.joda.time.{ Interval, LocalDateTime }
import org.scalatest.{ FlatSpecLike, GivenWhenThen, Matchers }

trait RollupMetaDaoHBaseTest extends HBaseTestBase with FlatSpecLike with Matchers with GivenWhenThen {

  private lazy val hbaseConnection = ConnectionFactory.createConnection(getConfiguration)

  "RollupMetaDaoHBase" should "handle invalid periods" in {
    val dao = new RollupMetaDaoHBase(hbaseConnection, "test")

    val invalidPeriods = List(
      Interval.parse("2020-01-01T13:00:00Z/2020-02-01T13:00:00Z"),
      Interval.parse("2020-01-11T13:00:00Z/2020-12-01T13:00:00Z")
    )
    When("invalid periods was put")
    dao.putInvalidPeriods(invalidPeriods)

    Then("returned periods must contains same data")
    val from = LocalDateTime.now().withHourOfDay(0)
    val to = LocalDateTime.now().withHourOfDay(0).withDayOfYear(from.getDayOfYear + 1)
    val result = dao.getInvalidPeriods(from, to)
    result should have size 2
    val in = invalidPeriods.head
    val period = result.head
    period.from shouldEqual in.getStart
    period.to shouldEqual in.getEnd
  }

}
