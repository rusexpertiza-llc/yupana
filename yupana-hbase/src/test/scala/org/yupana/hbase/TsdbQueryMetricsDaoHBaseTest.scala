package org.yupana.hbase

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.scalatest.GivenWhenThen
import org.yupana.api.query.Query
import org.yupana.core.{ TestDims, TestSchema }
import org.yupana.core.dao.QueryMetricsFilter
import org.yupana.core.model.MetricData
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.yupana.core.utils.metric.InternalMetricData
import org.yupana.metrics.QueryStates

import java.time.temporal.ChronoUnit
import java.time.{ OffsetDateTime, ZoneOffset }

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

    val startTime = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS)

    When("metric dao initialized")
    dao.saveQueryMetrics(
      List(
        InternalMetricData(
          query,
          None,
          startTime.toInstant.toEpochMilli,
          QueryStates.Running,
          0L,
          Map.empty,
          sparkQuery = false
        )
      )
    )

    Then("all metrics shall be zero")
    val qs = dao.queriesByFilter(Some(QueryMetricsFilter(queryId = Some(query.id)))).toList
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
      List(
        InternalMetricData(
          query,
          None,
          startTime.toInstant.toEpochMilli,
          QueryStates.Finished,
          10000000000L,
          Map("create_scans" -> MetricData(1, 2, 3)),
          sparkQuery = false
        )
      )
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
    mu.totalDuration shouldEqual 10000000000L
    mu.metrics("create_scans") shouldEqual MetricData(1, 2, 3)

    Then("No running queries available")
    dao.queriesByFilter(Some(QueryMetricsFilter(queryState = Some(QueryStates.Running)))) shouldBe empty
  }

  it should "handle multiple partitions" in {
    val dao = new TsdbQueryMetricsDaoHBase(hbaseConnection, "test")
    Given("Query")
    val query = Query(
      Some(TestSchema.testTable),
      Seq(dimension(TestDims.DIM_A).toField),
      None
    )

    val startTime = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MILLIS)

    When("metric dao initialized")
    dao.saveQueryMetrics(
      List(
        InternalMetricData(
          query,
          Some("1"),
          startTime.toInstant.toEpochMilli,
          QueryStates.Running,
          0L,
          Map.empty,
          sparkQuery = false
        )
      )
    )
    dao.saveQueryMetrics(
      List(
        InternalMetricData(
          query,
          Some("2"),
          startTime.plusSeconds(3).toInstant.toEpochMilli,
          QueryStates.Running,
          0L,
          Map.empty,
          sparkQuery = false
        )
      )
    )

    Then("all metrics shall be zero")
    val qs = dao.queriesByFilter(Some(QueryMetricsFilter(queryId = Some(query.id)))).toList
    qs should have size 1
    val m = qs.head
    m.queryId shouldEqual query.id
    m.state shouldEqual QueryStates.Running
    m.engine shouldEqual "STANDALONE"
    m.query shouldEqual query.toString
    m.totalDuration shouldEqual 3000000000L
    m.metrics.foreach { case (_, data) => data shouldEqual MetricData(0, 0, 0) }
    m.startDate shouldEqual startTime

    When("metrics are updated")
    dao.saveQueryMetrics(
      List(
        InternalMetricData(
          query,
          Some("1"),
          startTime.toInstant.toEpochMilli,
          QueryStates.Finished,
          4000000000L,
          Map("create_scans" -> MetricData(1, 2, 3)),
          sparkQuery = false
        )
      )
    )

    dao.saveQueryMetrics(
      List(
        InternalMetricData(
          query,
          Some("2"),
          startTime.plusSeconds(3).toInstant.toEpochMilli,
          QueryStates.Finished,
          2000000000L,
          Map("create_scans" -> MetricData(2, 3, 1)),
          sparkQuery = false
        )
      )
    )

    Then("it should return updated data")
    val qsu = dao.queriesByFilter(Some(QueryMetricsFilter(queryId = Some(query.id)))).toList
    qsu should have size 1
    val mu = qsu.head
    mu.startDate shouldEqual startTime
    mu.queryId shouldEqual query.id
    mu.state shouldEqual QueryStates.Finished
    mu.engine shouldEqual "STANDALONE"
    mu.query shouldEqual query.toString
    mu.totalDuration shouldEqual 5000000000L
    mu.metrics("create_scans") shouldEqual MetricData(1 + 2, 2 + 3, (1 + 2) / 5d)

    Then("No running queries available")
    dao.queriesByFilter(Some(QueryMetricsFilter(queryState = Some(QueryStates.Running)))) shouldBe empty
  }
}
