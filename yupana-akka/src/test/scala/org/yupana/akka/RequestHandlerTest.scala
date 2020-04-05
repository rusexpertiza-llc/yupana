package org.yupana.akka

import com.google.protobuf.ByteString
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory
import org.scalatest.{ EitherValues, FlatSpec, Inside, Matchers }
import org.yupana.api.Time
import org.yupana.api.query.Query
import org.yupana.api.types.Writable
import org.yupana.core.dao.{ QueryMetricsFilter, TsdbQueryMetricsDao }
import org.yupana.core.model.{ MetricData, QueryStates, TsdbQueryMetrics }
import org.yupana.core.{ QueryContext, TSDB, TsdbServerResult }
import org.yupana.proto.util.ProtocolVersion
import org.yupana.proto._
import org.yupana.schema.externallinks.ItemsInvertedIndex
import org.yupana.schema.{ Dimensions, ItemTableMetrics, SchemaRegistry, Tables }

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class RequestHandlerTest extends FlatSpec with Matchers with MockFactory with EitherValues with Inside {

  val requestHandler = new RequestHandler(SchemaRegistry.defaultSchema)

  "RequestHandler" should "send version on ping" in {
    val tsdb = mock[TSDB]
    val ping = Ping(1234567L, Some(Version(ProtocolVersion.value, 3, 1, "3.1.3-SNAPSHOT")))
    val resp = requestHandler.handlePingProto(tsdb, ping, 4, 2, "4.2.1").right.value.next()

    inside(resp) {
      case Response(Response.Resp.Pong(Pong(reqTime, _, Some(version)))) =>
        reqTime shouldEqual ping.reqTime
        version shouldEqual Version(ProtocolVersion.value, 4, 2, "4.2.1")
    }
  }

  it should "provide error on incorrect protocol version" in {
    val tsdb = mock[TSDB]
    val ping = Ping(1234567L, Some(Version(ProtocolVersion.value - 1, 2, 2, "2.2.2")))
    val err = requestHandler.handlePingProto(tsdb, ping, 4, 2, "4.2.1").left.value

    err shouldEqual s"Incompatible protocols: driver protocol ${ProtocolVersion.value - 1}, server protocol ${ProtocolVersion.value}"
  }

  it should "handle SqlQuery" in {
    import org.yupana.api.query.syntax.All._

    val tsdb = mock[TSDB]
    val query = SqlQuery(
      "SELECT item FROM items_kkm WHERE time >= ? AND time < ? AND ItemsInvertedIndex_phrase = ? AND sum = ? GROUP BY item",
      Seq(
        ParameterValue(1, Value(Value.Value.TimeValue(1234567L))),
        ParameterValue(2, Value(Value.Value.TimeValue(2345678L))),
        ParameterValue(4, Value(Value.Value.DecimalValue("300"))),
        ParameterValue(3, Value(Value.Value.TextValue("деталь")))
      )
    )

    val expected = Query(
      table = Tables.itemsKkmTable,
      fields = Seq(dimension(Dimensions.ITEM_TAG).toField),
      filter = and(
        ge(time, const(Time(1234567L))),
        lt(time, const(Time(2345678L))),
        equ(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD), const("деталь")),
        equ(metric(ItemTableMetrics.sumField), const(BigDecimal(300)))
      ),
      groupBy = Seq(dimension(Dimensions.ITEM_TAG))
    )

    val qc = QueryContext(expected, const(true))

    (tsdb.query _)
      .expects(expected)
      .returning(
        new TsdbServerResult(
          qc,
          Seq(
            Array[Option[Any]](Some("деталь от паровоза"))
          ).toIterator
        )
      )

    val resp = Await.result(requestHandler.handleQuery(tsdb, query), 2.seconds).right.value

    resp.next() shouldEqual Response(
      Response.Resp.ResultHeader(ResultHeader(Seq(ResultField("item", "VARCHAR")), Some("items_kkm")))
    )

    val data = resp.next()
    data shouldEqual Response(
      Response.Resp
        .Result(ResultChunk(Seq(ByteString.copyFrom(implicitly[Writable[String]].write("деталь от паровоза")))))
    )

    resp.next() shouldEqual Response(Response.Resp.ResultStatistics(ResultStatistics(-1, -1)))

    resp shouldBe empty
  }

  it should "fail on invalid SQL" in {
    val tsdb = mock[TSDB]
    val query = SqlQuery("INSERT 'сосиски' INTO kkm_items")

    val err = Await.result(requestHandler.handleQuery(tsdb, query), 2.seconds).left.value
    err should startWith("Invalid SQL statement")
  }

  it should "handle table list request" in {
    val tsdb = mock[TSDB]
    val query = SqlQuery("SHOW TABLES")

    val resp = Await.result(requestHandler.handleQuery(tsdb, query), 2.seconds).right.value.toList

    resp should have size SchemaRegistry.defaultSchema.tables.size + 2 // Header and footer
  }

  class MockedTsdb(metricsDao: TsdbQueryMetricsDao) extends TSDB(null, metricsDao, null, identity)

  it should "handle show queries request" in {
    val metricsDao = mock[TsdbQueryMetricsDao]
    val tsdb = new MockedTsdb(metricsDao)

    val metrics = Seq(
      "create_dimensions_filters",
      "create_scans",
      "scan",
      "load_tags",
      "filter_rows",
      "window_functions_check",
      "window_functions",
      "map_operation",
      "post_map_operation",
      "reduce_operation",
      "post_filter",
      "collect_result_rows",
      "dimension_values_for_ids",
      "read_external_links",
      "extract_data_computation",
      "parse_scan_result"
    )

    (metricsDao.queriesByFilter _)
      .expects(None, Some(3))
      .returning(
        Seq(
          TsdbQueryMetrics(
            1,
            "323232",
            new DateTime(2019, 11, 13, 0, 0),
            0,
            "SELECT kkm FROM kkm_items",
            QueryStates.Running,
            "standalone",
            metrics.zipWithIndex.map { case (m, i) => m -> MetricData(i, i * 5d, i * 7d) }.toMap
          )
        )
      )
    val query = SqlQuery("SHOW QUERIES LIMIT 3")
    val resp = Await.result(requestHandler.handleQuery(tsdb, query), 2.seconds).right.value.toList

    resp should have size 3
    val fields = resp(0).getResultHeader.fields.map(_.name)

    fields should contain theSameElementsAs metrics.flatMap(m => Seq(s"${m}_count", s"${m}_time", s"${m}_speed")) ++ Seq(
      "query_id",
      "engine",
      "state",
      "query",
      "start_date",
      "total_duration"
    )
  }

  it should "handle kill query request" in {
    val metricsDao = mock[TsdbQueryMetricsDao]
    val tsdb = new MockedTsdb(metricsDao)

    (metricsDao.setQueryState _).expects(QueryMetricsFilter(None, Some("12345"), None), QueryStates.Cancelled)
    val query = SqlQuery("KILL QUERY WHERE query_id = '12345'")
    val resp = Await.result(requestHandler.handleQuery(tsdb, query), 2.seconds).right.value.toList

    resp(1) shouldEqual Response(
      Response.Resp.Result(ResultChunk(Seq(ByteString.copyFrom(implicitly[Writable[String]].write("OK")))))
    )
  }

  it should "handle delete query metrics request" in {
    val metricsDao = mock[TsdbQueryMetricsDao]
    val tsdb = new MockedTsdb(metricsDao)

    (metricsDao.deleteMetrics _).expects(QueryMetricsFilter(None, None, Some(QueryStates.Cancelled))).returning(8)
    val query = SqlQuery("DELETE QUERIES WHERE state = 'CANCELLED'")
    val resp = Await.result(requestHandler.handleQuery(tsdb, query), 2.seconds).right.value.toList

    resp(1) shouldEqual Response(
      Response.Resp.Result(ResultChunk(Seq(ByteString.copyFrom(implicitly[Writable[Int]].write(8)))))
    )
  }

}
