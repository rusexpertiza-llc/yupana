package org.yupana.postgres

import org.postgresql.util.PSQLException
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.Query
import org.yupana.api.types.StringReaderWriter
import org.yupana.cache.CacheFactory
import org.yupana.core._
import org.yupana.core.auth.{ Authorizer, PermissionService, TsdbRole, YupanaUser }
import org.yupana.core.dao.ChangelogDao
import org.yupana.core.model.InternalQuery
import org.yupana.core.providers.JdbcMetadataProvider
import org.yupana.core.sql.{ FunctionRegistry, SqlQueryProcessor }
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.postgres.YupanaPostgresTest.TestAuthorizer
import org.yupana.postgres.protocol.PostgresStringReaderWriter
import org.yupana.settings.Settings
import org.yupana.utils.RussianTokenizer

import java.sql.{ DriverManager, Timestamp, Types }
import java.time.LocalDateTime
import java.util.Properties

class YupanaPostgresTest extends AnyFlatSpec with Matchers with MockFactory with BeforeAndAfterAll {
  println(Class.forName("org.postgresql.Driver").getName)

  implicit private val calculator: ConstantCalculator = new ConstantCalculator(RussianTokenizer)

  override protected def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(Settings(properties))
  }

  "Postgres" should "connect" in withServerStarted { (server, _) =>
    val port = server.getPort
    val conn = DriverManager.getConnection(s"jdbc:postgresql://localhost:$port/", "test", "12345")
    conn.isValid(0) shouldBe true
    conn.close()
  }

  it should "fail to connect if credentials are invalid" in withServerStarted { (server, _) =>
    val port = server.getPort
    the[PSQLException] thrownBy DriverManager.getConnection(
      s"jdbc:postgresql://localhost:$port/",
      "admin",
      "admin"
    ) should have message "ERROR: Invalid user or password"
  }

  it should "execute queries" in withServerStarted { (server, _) =>
    val port = server.getPort
    val conn = DriverManager.getConnection(s"jdbc:postgresql://localhost:$port/", "test", "12345")
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT 5 + 4, trunc_month(TIMESTAMP '2024-03-31 12:33:11'), 'aaa' + 'bbb', 45 / 2")
    rs.getMetaData.getColumnCount shouldEqual 4
    rs.next() shouldBe true
    rs.getMetaData.getColumnType(1) shouldEqual Types.NUMERIC
    rs.getInt(1) shouldEqual 9

    rs.getMetaData.getColumnType(2) shouldEqual Types.TIMESTAMP
    rs.getTimestamp(2) shouldEqual Timestamp.valueOf(LocalDateTime.of(2024, 3, 1, 0, 0, 0))

    rs.getMetaData.getColumnType(3) shouldEqual Types.VARCHAR
    rs.getString(3) shouldEqual "aaabbb"

    rs.getMetaData.getColumnType(4) shouldEqual Types.NUMERIC
    rs.getDouble(4) shouldEqual 22.5d
  }

  it should "handle prepared statements" in withServerStarted { (server, dao) =>

    import org.yupana.api.query.syntax.All._

    val from = LocalDateTime.of(2024, 4, 1, 21, 29, 27)
    val to = LocalDateTime.of(2024, 5, 1, 0, 0, 0)

    (dao.query _)
      .expects(
        InternalQuery(
          TestSchema.testTable,
          Set(
            time,
            metric(TestTableFields.TEST_FIELD),
            metric(TestTableFields.TEST_LONG_FIELD),
            metric(TestTableFields.TEST_STRING_FIELD)
          ),
          and(
            ge(time, const(Time(from))),
            lt(time, const(Time(to))),
            equ(lower(dimension(TestDims.DIM_A)), const("test me"))
          )
        ),
        *,
        *
      )
      .onCall((_, b, _) =>
        Iterator(
          b.set(Time(LocalDateTime.of(2024, 4, 2, 3, 4, 5)))
            .set(metric(TestTableFields.TEST_FIELD), 33.3d)
            .set(metric(TestTableFields.TEST_LONG_FIELD), 55L)
            .set(metric(TestTableFields.TEST_STRING_FIELD), "reply")
            .buildAndReset()
        )
      )

    val port = server.getPort
    val conn = DriverManager.getConnection(s"jdbc:postgresql://localhost:$port/", "test", "12345")
    val stmt = conn.prepareStatement("""SELECT testField, testLongField, testStringField
        |  FROM test_table
        |  WHERE A = ? AND TIME >= ? and TIME < ?""".stripMargin)

    stmt.setString(1, "test me")
    stmt.setTimestamp(2, Timestamp.valueOf(from))
    stmt.setTimestamp(3, Timestamp.valueOf(to))
    val rs = stmt.executeQuery()
    rs.next()

    rs.getMetaData.getColumnType(1) shouldEqual Types.DOUBLE
    rs.getDouble(1) shouldEqual 33.3d

    rs.getMetaData.getColumnType(2) shouldEqual Types.BIGINT
    rs.getDouble(2) shouldEqual 55L

    rs.getMetaData.getColumnType(3) shouldEqual Types.VARCHAR
    rs.getString(3) shouldEqual "reply"
  }

  it should "provide error on invalid queries" in withServerStarted { (server, _) =>
    val port = server.getPort
    val conn = DriverManager.getConnection(s"jdbc:postgresql://localhost:$port/", "test", "12345")
    val stmt = conn.createStatement()
    (the[PSQLException] thrownBy stmt.executeQuery("SELECT 5 x 4")).getMessage should include(
      "Invalid SQL statement: 'SELECT 5 x 4'"
    )
  }
  }

  def withServerStarted(body: (YupanaPostgres, TSTestDao) => Any): Unit = {
    implicit val srw: StringReaderWriter = PostgresStringReaderWriter
    val fr = new FunctionRegistry()
    val jmp = new JdbcMetadataProvider(TestSchema.schema, fr, 1, 2, "1.2.3")
    val sqp = new SqlQueryProcessor(TestSchema.schema, fr)
    val tsdbDaoMock = TsdbMocks.daoMock
    val changelogDaoMock = mock[ChangelogDao]
    val tsdb = new TSDB(
      TestSchema.schema,
      tsdbDaoMock,
      changelogDaoMock,
      identity,
      SimpleTsdbConfig(),
      { (_: Query, _: String) => NoMetricCollector }
    )

    val queryEngine = new QueryEngineRouter(tsdb, null, jmp, sqp, new PermissionService(putEnabled = true), null)

    val server = new YupanaPostgres("localhost", 0, 4, PgContext(queryEngine, new TestAuthorizer))
    server.start()
    body(server, tsdbDaoMock)
    server.stop()
  }
}

object YupanaPostgresTest {
  class TestAuthorizer extends Authorizer {
    override def authorize(userName: Option[String], password: Option[String]): Either[String, YupanaUser] = {
      (userName, password) match {
        case (Some("test"), Some("12345")) => Right(YupanaUser("test", Some("12354"), TsdbRole.ReadWrite))
        case _                             => Left("Invalid user or password")
      }
    }
  }
}
