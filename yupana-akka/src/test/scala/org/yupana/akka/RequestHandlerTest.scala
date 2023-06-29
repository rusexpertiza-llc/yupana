package org.yupana.akka

import com.google.protobuf.ByteString
import org.scalamock.scalatest.MockFactory
import org.scalatest.{ BeforeAndAfterAll, EitherValues, Inside }
import org.yupana.api.Time
import org.yupana.api.query.{ DataPoint, Query }
import org.yupana.api.schema.MetricValue
import org.yupana.api.types.Storable
import org.yupana.core.dao.{ ChangelogDao, QueryMetricsFilter, TsdbQueryMetricsDao }
import org.yupana.core.model.{ MetricData, TsdbQueryMetrics }
import org.yupana.core._
import org.yupana.core.providers.JdbcMetadataProvider
import org.yupana.core.sql.SqlQueryProcessor
import org.yupana.core.utils.metric.{ PersistentMetricQueryReporter, StandaloneMetricCollector }
import org.yupana.core.{ QueryContext, SimpleTsdbConfig, TSDB, TsdbServerResult }
import org.yupana.proto._
import org.yupana.proto.util.ProtocolVersion
import org.yupana.schema.externallinks.ItemsInvertedIndex
import org.yupana.schema.{ Dimensions, ItemTableMetrics, SchemaRegistry, Tables }

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.cache.CacheFactory
import org.yupana.core.auth.YupanaUser
import org.yupana.metrics.QueryStates
import org.yupana.settings.Settings

import java.time.{ OffsetDateTime, ZoneOffset }
import java.util.Properties

class RequestHandlerTest
    extends AnyFlatSpec
    with Matchers
    with MockFactory
    with EitherValues
    with Inside
    with BeforeAndAfterAll {

  private val sqlQueryProcessor = new SqlQueryProcessor(SchemaRegistry.defaultSchema)
  private val jdbcMetadataProvider = new JdbcMetadataProvider(SchemaRegistry.defaultSchema)

  override protected def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(Settings(properties))
  }

  "RequestHandler" should "send version on ping" in {
    val ping = Ping(1234567L, Some(Version(ProtocolVersion.value, 3, 1, "3.1.3-SNAPSHOT")))
    val requestHandler = new RequestHandler(mock[QueryEngineRouter])
    val resp = requestHandler.handlePingProto(ping, 4, 2, "4.2.1").value.next()

    inside(resp) {
      case Response(Response.Resp.Pong(Pong(reqTime, _, Some(version)))) =>
        reqTime shouldEqual ping.reqTime
        version shouldEqual Version(ProtocolVersion.value, 4, 2, "4.2.1")
    }
  }

  it should "provide error on incorrect protocol version" in {
    val ping = Ping(1234567L, Some(Version(ProtocolVersion.value - 1, 2, 2, "2.2.2")))
    val requestHandler = new RequestHandler(mock[QueryEngineRouter])
    val err = requestHandler.handlePingProto(ping, 4, 2, "4.2.1").left.value

    err shouldEqual s"Incompatible protocols: driver protocol ${ProtocolVersion.value - 1}, server protocol ${ProtocolVersion.value}"
  }

  it should "handle SqlQuery" in {
    import org.yupana.api.query.syntax.All._

    val tsdb = mock[TSDB]
    val queryEngineRouter = new QueryEngineRouter(
      new TimeSeriesQueryEngine(tsdb),
      mock[FlatQueryEngine],
      jdbcMetadataProvider,
      sqlQueryProcessor
    )
    val query = SqlQuery(
      "SELECT item FROM items_kkm WHERE time >= ? AND time < ? AND ItemsInvertedIndex_phrase = ? AND sum = ? GROUP BY item",
      Seq(
        ParameterValue(1, Value(Value.Value.TimeValue(1234567L))),
        ParameterValue(2, Value(Value.Value.TimeValue(2345678L))),
        ParameterValue(4, Value(Value.Value.DecimalValue("300"))),
        ParameterValue(3, Value(Value.Value.TextValue("Деталь")))
      )
    )

    val expected = Query(
      table = Some(Tables.itemsKkmTable),
      fields = Seq(dimension(Dimensions.ITEM).toField),
      filter = Some(
        and(
          ge(time, const(Time(1234567L))),
          lt(time, const(Time(2345678L))),
          equ(lower(link(ItemsInvertedIndex, ItemsInvertedIndex.PHRASE_FIELD)), const("деталь")),
          equ(metric(ItemTableMetrics.sumField), const(BigDecimal(300)))
        )
      ),
      groupBy = Seq(dimension(Dimensions.ITEM))
    )

    val qc = new QueryContext(expected, None, ExpressionCalculatorFactory)

    (tsdb.query _)
      .expects(expected)
      .returning(
        new TsdbServerResult(
          qc,
          Seq(
            Array[Any]("деталь от паровоза")
          ).iterator
        )
      )

    val requestHandler = new RequestHandler(queryEngineRouter)
    val resp = Await.result(requestHandler.handleQuery(query), 20.seconds).value

    resp.next() shouldEqual Response(
      Response.Resp.ResultHeader(ResultHeader(Seq(ResultField("item", "VARCHAR")), Some("items_kkm")))
    )

    val data = resp.next()
    data shouldEqual Response(
      Response.Resp.Result(
        ResultChunk(Seq(ByteString.copyFrom(implicitly[Storable[String]].write("деталь от паровоза"))))
      )
    )

    resp.next() shouldEqual Response(Response.Resp.ResultStatistics(ResultStatistics(-1, -1)))

    resp shouldBe empty
  }

  it should "fail on empty values" in {
    val queryEngineRouter = mock[QueryEngineRouter]
    val query = SqlQuery(
      "SELECT item FROM items_kkm WHERE time >= ? AND time < ? AND ItemsInvertedIndex_phrase = ? AND sum = ? GROUP BY item",
      Seq(
        ParameterValue(1, Value(Value.Value.TimeValue(1234567L))),
        ParameterValue(2, Value(Value.Value.TimeValue(2345678L))),
        ParameterValue(4, Value(Value.Value.Empty)),
        ParameterValue(3, Value(Value.Value.TextValue("деталь")))
      )
    )

    val requestHandler = new RequestHandler(queryEngineRouter)
    an[IllegalArgumentException] should be thrownBy Await.result(
      requestHandler.handleQuery(query),
      20.seconds
    )
  }

  it should "handle batch upserts" in {
    val tsdb = mock[TSDB]
    val queryEngineRouter = new QueryEngineRouter(
      new TimeSeriesQueryEngine(tsdb),
      mock[FlatQueryEngine],
      jdbcMetadataProvider,
      sqlQueryProcessor
    )
    val query = BatchSqlQuery(
      "UPSERT INTO items_kkm (kkmId, item, operation_type, position, time, sum, quantity) VALUES (?, ?, ?, ?, ?, ?, ?)",
      Seq(
        ParameterValues(
          Seq(
            ParameterValue(1, Value(Value.Value.DecimalValue("12345"))),
            ParameterValue(2, Value(Value.Value.TextValue("thing one"))),
            ParameterValue(3, Value(Value.Value.DecimalValue("1"))),
            ParameterValue(4, Value(Value.Value.DecimalValue("1"))),
            ParameterValue(5, Value(Value.Value.TimeValue(1578426233000L))),
            ParameterValue(6, Value(Value.Value.DecimalValue("100"))),
            ParameterValue(7, Value(Value.Value.DecimalValue("1")))
          )
        ),
        ParameterValues(
          Seq(
            ParameterValue(1, Value(Value.Value.DecimalValue("12345"))),
            ParameterValue(2, Value(Value.Value.TextValue("thing two"))),
            ParameterValue(3, Value(Value.Value.DecimalValue("1"))),
            ParameterValue(4, Value(Value.Value.DecimalValue("2"))),
            ParameterValue(5, Value(Value.Value.TimeValue(1578426233000L))),
            ParameterValue(6, Value(Value.Value.DecimalValue("300"))),
            ParameterValue(7, Value(Value.Value.DecimalValue("2")))
          )
        )
      )
    )

    (tsdb.put _).expects(where { (dps, user) =>
      dps.toSeq == Seq(
        DataPoint(
          Tables.itemsKkmTable,
          1578426233000L,
          Map(
            Dimensions.ITEM -> "thing one",
            Dimensions.KKM_ID -> 12345,
            Dimensions.POSITION -> 1.toShort,
            Dimensions.OPERATION_TYPE -> 1.toByte
          ),
          Seq(MetricValue(ItemTableMetrics.quantityField, 1d), MetricValue(ItemTableMetrics.sumField, BigDecimal(100)))
        ),
        DataPoint(
          Tables.itemsKkmTable,
          1578426233000L,
          Map(
            Dimensions.ITEM -> "thing two",
            Dimensions.KKM_ID -> 12345,
            Dimensions.POSITION -> 2.toShort,
            Dimensions.OPERATION_TYPE -> 1.toByte
          ),
          Seq(MetricValue(ItemTableMetrics.quantityField, 2d), MetricValue(ItemTableMetrics.sumField, BigDecimal(300)))
        )
      ) && user == YupanaUser.ANONYMOUS
    })

    val requestHandler = new RequestHandler(queryEngineRouter)
    val resp =
      Await.result(requestHandler.handleBatchQuery(query), 20.seconds).value.toList

    resp should contain theSameElementsInOrderAs Seq(
      Response(Response.Resp.ResultHeader(ResultHeader(Seq(ResultField("RESULT", "VARCHAR")), Some("RESULT")))),
      Response(Response.Resp.Result(ResultChunk(Seq(ByteString.copyFrom(implicitly[Storable[String]].write("OK")))))),
      Response(Response.Resp.ResultStatistics(ResultStatistics(-1, -1)))
    )
  }

  it should "fail on invalid SQL" in {
    val queryEngineRouter = new QueryEngineRouter(
      mock[TimeSeriesQueryEngine],
      mock[FlatQueryEngine],
      jdbcMetadataProvider,
      sqlQueryProcessor
    )
    val sql = "INSERT 'сосиски' INTO kkm_items"

    val err = queryEngineRouter.query(sql, Map.empty[Int, org.yupana.core.sql.parser.Value]).left.value
    err should startWith("Invalid SQL statement")
  }

  it should "handle table list request" in {
    val queryEngineRouter = new QueryEngineRouter(
      mock[TimeSeriesQueryEngine],
      mock[FlatQueryEngine],
      jdbcMetadataProvider,
      sqlQueryProcessor
    )
    val query = SqlQuery("SHOW TABLES")

    val requestHandler = new RequestHandler(queryEngineRouter)
    val resp = Await.result(requestHandler.handleQuery(query), 20.seconds).value.toList

    resp should have size SchemaRegistry.defaultSchema.tables.size + 2 // Header and footer
  }

  class MockedTsdb
      extends TSDB(
        SchemaRegistry.defaultSchema,
        null,
        null,
        identity,
        SimpleTsdbConfig(),
        { q: Query =>
          new StandaloneMetricCollector(
            q,
            "test",
            5,
            new PersistentMetricQueryReporter(mockFunction[TsdbQueryMetricsDao], forceSaving = true)
          )
        }
      )

  it should "handle show queries request" in {
    val metricsDao = mock[TsdbQueryMetricsDao]
    val queryEngineRouter = new QueryEngineRouter(
      mock[TimeSeriesQueryEngine],
      new FlatQueryEngine(metricsDao, mock[ChangelogDao]),
      jdbcMetadataProvider,
      sqlQueryProcessor
    )

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
      "parse_scan_result",
      "dictionary_scan"
    )

    (metricsDao.queriesByFilter _)
      .expects(None, Some(3))
      .returning(
        Iterator(
          TsdbQueryMetrics(
            "323232",
            None,
            OffsetDateTime.of(2019, 11, 13, 0, 0, 0, 0, ZoneOffset.UTC),
            0,
            "SELECT kkm FROM kkm_items",
            QueryStates.Running,
            "standalone",
            metrics.zipWithIndex.map { case (m, i) => m -> MetricData(i, i * 5000000L, i * 7d) }.toMap
          )
        )
      )
    val query = SqlQuery("SHOW QUERIES LIMIT 3")
    val requestHandler = new RequestHandler(queryEngineRouter)
    val resp = Await.result(requestHandler.handleQuery(query), 20.seconds).value.toList

    resp should have size 3
    val fields = resp(0).getResultHeader.fields.map(_.name)

    fields should contain theSameElementsAs metrics.flatMap(m =>
      Seq(s"${m}_count", s"${m}_time", s"${m}_speed")
    ) ++ Seq(
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
    val queryEngineRouter = new QueryEngineRouter(
      mock[TimeSeriesQueryEngine],
      new FlatQueryEngine(metricsDao, mock[ChangelogDao]),
      jdbcMetadataProvider,
      sqlQueryProcessor
    )

    val query = SqlQuery("KILL QUERY WHERE query_id = '12345'")
    val requestHandler = new RequestHandler(queryEngineRouter)
    val resp = Await.result(requestHandler.handleQuery(query), 20.seconds).value.toList

    resp(1) shouldEqual Response(
      Response.Resp.Result(ResultChunk(Seq(ByteString.copyFrom(implicitly[Storable[String]].write("OK")))))
    )
  }

  it should "handle delete query metrics request" in {
    val metricsDao = mock[TsdbQueryMetricsDao]
    val queryEngineRouter = new QueryEngineRouter(
      mock[TimeSeriesQueryEngine],
      new FlatQueryEngine(metricsDao, mock[ChangelogDao]),
      jdbcMetadataProvider,
      sqlQueryProcessor
    )

    (metricsDao.deleteMetrics _).expects(QueryMetricsFilter(None, Some(QueryStates.Cancelled))).returning(8)
    val query = SqlQuery("DELETE QUERIES WHERE state = 'CANCELLED'")
    val requestHandler = new RequestHandler(queryEngineRouter)
    val resp = Await.result(requestHandler.handleQuery(query), 20.seconds).value.toList

    resp(1) shouldEqual Response(
      Response.Resp.Result(ResultChunk(Seq(ByteString.copyFrom(implicitly[Storable[Int]].write(8)))))
    )
  }

  it should "handle show updated intervals" in {
    val changelogDao = mock[ChangelogDao]
    val queryEngineRouter = new QueryEngineRouter(
      mock[TimeSeriesQueryEngine],
      new FlatQueryEngine(mock[TsdbQueryMetricsDao], changelogDao),
      jdbcMetadataProvider,
      sqlQueryProcessor
    )

    (changelogDao.getUpdatesIntervals _)
      .expects(Some("a_table"), None, None, None, None, Some("John Doe"))
      .returning(Seq.empty)

    val query = SqlQuery("SHOW updates_intervals WHERE table = 'a_table' AND updated_by='John Doe'")
    val requestHandler = new RequestHandler(queryEngineRouter)
    val resp = Await.result(requestHandler.handleQuery(query), 20.seconds).value.toList

    resp should have size 2

    resp.head.getResultHeader.tableName shouldEqual Some("UPDATES_INTERVALS")
    resp.head.getResultHeader.fields.map(_.name) should contain theSameElementsAs List(
      "table",
      "updated_at",
      "from",
      "to",
      "updated_by"
    )
  }
}
