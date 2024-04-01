package org.yupana.postgres

import org.postgresql.Driver
import org.postgresql.util.PSQLException
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.query.Query
import org.yupana.api.types.StringReaderWriter
import org.yupana.cache.CacheFactory
import org.yupana.core.auth.{ Authorizer, PermissionService, TsdbRole, YupanaUser }
import org.yupana.core.dao.ChangelogDao
import org.yupana.core.providers.JdbcMetadataProvider
import org.yupana.core.sql.{ FunctionRegistry, SqlQueryProcessor }
import org.yupana.core.utils.metric.NoMetricCollector
import org.yupana.core._
import org.yupana.postgres.YupanaPostgresTest.TestAuthorizer
import org.yupana.postgres.protocol.PostgresStringReaderWriter
import org.yupana.settings.Settings

import java.sql.{ DriverManager, Types }
import java.util.Properties

class YupanaPostgresTest extends AnyFlatSpec with Matchers with MockFactory with BeforeAndAfterAll {
  println(classOf[Driver].getName)

  override protected def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(Settings(properties))
  }

  "Postgres" should "connect" in withServerStarted { (server, _) =>
    val port = server.getPort
    val conn = DriverManager.getConnection(s"jdbc:postgresql://localhost:$port/yupana", "test", "12345")
    conn.isValid(0) shouldBe true
    conn.close()
  }

  it should "fail to connect if credentials are invalid" in withServerStarted { (server, _) =>
    val port = server.getPort
    the[PSQLException] thrownBy DriverManager.getConnection(
      s"jdbc:postgresql://localhost:$port/yupana",
      "admin",
      "admin"
    ) should have message "ERROR: Invalid user or password"
  }

  it should "execute queries" in withServerStarted { (server, _) =>
    val port = server.getPort
    val conn = DriverManager.getConnection(s"jdbc:postgresql://localhost:$port/yupana", "test", "12345")
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT 5 + 4")
    rs.next() shouldBe true
    rs.getInt(1) shouldEqual 9
    rs.getMetaData.getColumnType(1) shouldEqual Types.NUMERIC
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
