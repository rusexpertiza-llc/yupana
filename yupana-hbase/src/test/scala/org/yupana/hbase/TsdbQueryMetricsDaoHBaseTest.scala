package org.yupana.hbase

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.joda.time.DateTime
import org.scalatest.GivenWhenThen
import org.yupana.api.query.Query
import org.yupana.core.{ TestDims, TestSchema }
import org.yupana.core.dao.QueryMetricsFilter
import org.yupana.core.model.{ MetricData, QueryStates }
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

trait TsdbQueryMetricsDaoHBaseTest extends HBaseTestBase with AnyFlatSpecLike with Matchers with GivenWhenThen {

  import org.yupana.api.query.syntax.All._

  private lazy val hbaseConnection = ConnectionFactory.createConnection(getConfiguration)

  "TsdbQueryMetricsDaoHBase" should "process single query" in {
    val dao = new TsdbQueryMetricsDaoHBase(hbaseConnection, "test")
    Given("Query")
    val query = Query(
      Some(TestSchema.testTable),
      Seq(dimension(TestDims.DIM_A).toField),
      None
    )

    val startTime = DateTime.now

    When("metric dao initialized")
    dao.saveQueryMetrics(query, 0, startTime.getMillis, QueryStates.Running, 0d, Map.empty, sparkQuery = false)

    Then("all metrics shall be zero")
    val qs = dao.queriesByFilter(Some(QueryMetricsFilter(queryId = Some(query.id), partitionId = Some(0)))).toList
    qs should have size 1
    val m = qs.head
    m.queryId shouldEqual query.id
    m.state shouldEqual QueryStates.Running
    m.engine shouldEqual "STANDALONE"
    m.query shouldEqual query.toString
    m.totalDuration shouldEqual 0d
    m.metrics.foreach { case (_, data) => data shouldEqual MetricData(0, 0, 0) }
    m.startDate shouldEqual startTime

    When("metrics are updated")
    dao.saveQueryMetrics(
      query,
      0,
      startTime.getMillis,
      QueryStates.Finished,
      42d,
      Map("create_scans" -> MetricData(1, 2, 3)),
      sparkQuery = false
    )

    Then("it should return updated data")
    val qsu = dao.queriesByFilter(Some(QueryMetricsFilter(queryState = Some(QueryStates.Finished)))).toList
    qsu should have size 1
    val mu = qsu.head
    mu.startDate shouldEqual startTime
    mu.queryId shouldEqual query.id
    mu.state shouldEqual QueryStates.Finished
    mu.engine shouldEqual "STANDALONE"
    mu.query shouldEqual query.toString
    mu.totalDuration shouldEqual 42d
    mu.metrics("create_scans") shouldEqual MetricData(1, 2, 3)

    Then("No running queries available")
    dao.queriesByFilter(Some(QueryMetricsFilter(queryState = Some(QueryStates.Running)))) shouldBe empty
  }

}
