package org.yupana.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{ BeforeAndAfterAll, EitherValues }
import org.yupana.api.Time
import org.yupana.api.types.DataType
import org.yupana.cache.CacheFactory
import org.yupana.core.auth.{ PermissionService, TsdbRole, UserManager, YupanaUser }
import org.yupana.core.dao.{ ChangelogDao, QueryMetricsFilter, TsdbQueryMetricsDao, UserDao }
import org.yupana.core.model.UpdateInterval
import org.yupana.core.providers.JdbcMetadataProvider
import org.yupana.core.sql.{ FunctionRegistry, SqlQueryProcessor }
import org.yupana.core.sql.parser.TypedValue
import org.yupana.settings.Settings

import java.time.OffsetDateTime
import java.util.Properties

class QueryEngineRouterTest extends AnyFlatSpec with Matchers with TsdbMocks with EitherValues with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("app.properties"))
    CacheFactory.init(Settings(properties))
  }

  "QueryEngineRouter" should "handle selects" in withEngineRouter { (qer, _, _, _, _) =>
    val res = qer.query(YupanaUser("test", None, TsdbRole.ReadOnly), "SELECT 1", Map.empty).value.toList
    res should have size 1
    res.head.get[BigDecimal](0) shouldEqual 1
  }

  it should "not allow select for disabled user" in withEngineRouter { (qer, _, _, _, _) =>
    val err = qer.query(YupanaUser("test 2", None, TsdbRole.Disabled), "SELECT 1", Map.empty).left.value
    err shouldEqual "User test 2 doesn't have enough permissions"
  }

  it should "check permissions for upsert" in withEngineRouter { (qer, _, _, _, _) =>
    val err = qer
      .query(
        YupanaUser("test", None, TsdbRole.ReadWrite),
        "UPSERT INTO test_table(time, A, B, testField, testStringField) VALUES(TIMESTAMP '2024-02-26', 'A', 2, 5, 'one')",
        Map.empty
      )
      .left
      .value

    err shouldEqual "User test doesn't have enough permissions"
  }

  it should "check permissions for batch upsert" in withEngineRouter { (qer, _, _, _, _) =>
    val err = qer
      .batchQuery(
        YupanaUser("test", None, TsdbRole.ReadWrite),
        "UPSERT INTO test_table(time, A, B, testField, testStringField) VALUES(?, ?, ?, ?, ?)",
        List(
          Map(
            1 -> TypedValue(Time(OffsetDateTime.now())),
            2 -> TypedValue("A"),
            3 -> TypedValue(2),
            4 -> TypedValue(5),
            5 -> TypedValue("one")
          )
        )
      )
      .left
      .value

    err shouldEqual "User test doesn't have enough permissions"
  }

  it should "handle show tables" in withEngineRouter { (qer, _, _, _, _) =>
    val res = qer.query(YupanaUser("test", None, TsdbRole.ReadOnly), "SHOW TABLES", Map.empty).value.toList
    res.map(_.get[String]("TABLE_NAME")) should contain theSameElementsAs TestSchema.schema.tables.values.map(_.name)
  }

  it should "handle show version" in withEngineRouter { (qer, _, _, _, _) =>
    val res = qer.query(YupanaUser("test", None, TsdbRole.ReadOnly), "SHOW VERSION", Map.empty).value.toList
    res should have size 1
    res.head.get[String]("VERSION") shouldEqual "1.2.3"
  }

  it should "handle show columns" in withEngineRouter { (qer, _, _, _, _) =>
    val res =
      qer.query(YupanaUser("test", None, TsdbRole.ReadOnly), "SHOW COLUMNS FROM test_table", Map.empty).value.toList

    val fieldNames = List("time") ++ TestSchema.testTable.dimensionSeq.map(_.name) ++
      TestSchema.testTable.metrics.map(_.name) ++
      TestSchema.testTable.externalLinks.flatMap(e => e.fields.map(f => e.linkName + "_" + f.name))

    res.map(_.get[String]("COLUMN_NAME")) should contain theSameElementsAs fieldNames
  }

  it should "handle show functions" in withEngineRouter { (qer, _, _, _, _) =>
    val res =
      qer.query(YupanaUser("test", None, TsdbRole.ReadOnly), "SHOW FUNCTIONS FOR VARCHAR", Map.empty).value.toList
    res.map(_.get[String]("NAME")) shouldEqual FunctionRegistry.functionsForType(DataType[String])
  }

  it should "handle list queries" in withEngineRouter { (qer, _, metricsDao, _, _) =>
    (metricsDao.queriesByFilter _).expects(None, None).returning(Iterator())
    val res = qer.query(YupanaUser("test", None, TsdbRole.ReadOnly), "SHOW QUERIES", Map.empty).value.toList
    res should have size 0
  }

  it should "handle delete metrics" in withEngineRouter { (qer, _, metricsDao, _, _) =>
    (metricsDao.deleteMetrics _).expects(QueryMetricsFilter(Some("123"))).returning(2)
    val res =
      qer
        .query(YupanaUser("test", None, TsdbRole.Admin), "DELETE QUERIES WHERE query_id = '123'", Map.empty)
        .value
        .toList
    res should have size 1
    res.head.get[Int]("DELETED") shouldEqual 2
  }

  it should "handle show query intervals" in withEngineRouter { (qer, _, _, changelogDao, _) =>
    val now = OffsetDateTime.now()
    (changelogDao.getUpdatesIntervals _)
      .expects(Some("receipt"), None, None, None, None, None)
      .returning(List(UpdateInterval("receipt", now.minusWeeks(1), now, now.minusDays(3), "user x")))
    val res =
      qer
        .query(
          YupanaUser("test", None, TsdbRole.Admin),
          s"""SHOW UPDATES_INTERVALS
           |  WHERE table = 'receipt'
           """.stripMargin,
          Map.empty
        )
        .value
        .toList
    res should have size 1
  }

  it should "list users" in withEngineRouter { (qer, _, _, _, userDao) =>
    (userDao.listUsers _)
      .expects()
      .returning(List(YupanaUser("test", None, TsdbRole.Admin), YupanaUser("test 2", None, TsdbRole.ReadWrite)))

    val res = qer.query(YupanaUser("test", None, TsdbRole.Admin), "SHOW USERS", Map.empty).value.toList
    res should have size 2
    res.map(_.get[String]("NAME")) should contain theSameElementsAs List("test", "test 2")
  }

  it should "prevent list users for non admins" in withEngineRouter { (qer, _, _, _, _) =>
    val err = qer.query(YupanaUser("test 3", None, TsdbRole.ReadWrite), "SHOW USERS", Map.empty).left.value
    err shouldEqual "User test 3 doesn't have enough permissions"
  }

  it should "create user" in withEngineRouter { (qer, _, _, _, userDao) =>
    (userDao.createUser _)
      .expects("test3", *, TsdbRole.ReadOnly)
      .returning(true)

    val res = qer
      .query(
        YupanaUser("test", None, TsdbRole.Admin),
        "CREATE USER 'test3' WITH PASSWORD '123' WITH ROLE 'read_only'",
        Map.empty
      )
      .value
      .toList
    res should have size 1
    res.head.get[String]("STATUS") shouldEqual "OK"
  }

  it should "prevent create users for non admins" in withEngineRouter { (qer, _, _, _, _) =>
    val err = qer
      .query(
        YupanaUser("test 3", None, TsdbRole.ReadWrite),
        "CREATE USER 'test3' WITH PASSWORD '123' WITH ROLE 'read_only'",
        Map.empty
      )
      .left
      .value
    err shouldEqual "User test 3 doesn't have enough permissions"
  }

  it should "alter user" in withEngineRouter { (qer, _, _, _, userDao) =>
    (userDao.updateUser _)
      .expects("test3", *, None)
      .returning(true)

    val res = qer
      .query(
        YupanaUser("test", None, TsdbRole.Admin),
        "ALTER USER 'test3' SET PASSWORD='321'",
        Map.empty
      )
      .value
      .toList
    res should have size 1
    res.head.get[String]("STATUS") shouldEqual "OK"
  }

  it should "prevent modify users for non admins" in withEngineRouter { (qer, _, _, _, _) =>
    val err = qer
      .query(
        YupanaUser("test 3", None, TsdbRole.ReadWrite),
        "ALTER USER 'test3' SET PASSWORD='321'",
        Map.empty
      )
      .left
      .value
    err shouldEqual "User test 3 doesn't have enough permissions"
  }

  it should "drop user" in withEngineRouter { (qer, _, _, _, userDao) =>
    (userDao.deleteUser _)
      .expects("test3")
      .returning(true)

    val res = qer.query(YupanaUser("test", None, TsdbRole.Admin), "DROP USER 'test3'", Map.empty).value.toList
    res should have size 1
    res.head.get[String]("STATUS") shouldEqual "OK"
  }

  it should "inform if user not found" in withEngineRouter { (qer, _, _, _, userDao) =>
    (userDao.deleteUser _)
      .expects("test3")
      .returning(false)

    val err = qer.query(YupanaUser("test", None, TsdbRole.Admin), "DROP USER 'test3'", Map.empty).left.value
    err shouldEqual "User not found"
  }

  it should "prevent drop users for non admins" in withEngineRouter { (qer, _, _, _, _) =>
    val err = qer.query(YupanaUser("test 3", None, TsdbRole.ReadWrite), "DROP USER 'test4'", Map.empty).left.value
    err shouldEqual "User test 3 doesn't have enough permissions"
  }

  def withEngineRouter(f: (QueryEngineRouter, TSTestDao, TsdbQueryMetricsDao, ChangelogDao, UserDao) => Unit): Unit =
    withTsdbMock { (tsdb, tsdbDao) =>
      val metricsDao = mock[TsdbQueryMetricsDao]
      val changelogDao = mock[ChangelogDao]
      val userDao = mock[UserDao]
      val fqe = new FlatQueryEngine(metricsDao, changelogDao)
      val jmp = new JdbcMetadataProvider(TestSchema.schema, 1, 2, "1.2.3")
      val sqp = new SqlQueryProcessor(TestSchema.schema)
      val ps = new PermissionService(putEnabled = false)
      val um = new UserManager(userDao, Some("admin"), Some("admin"))

      val qer = new QueryEngineRouter(tsdb, fqe, jmp, sqp, ps, um)

      f(qer, tsdbDao, metricsDao, changelogDao, userDao)
    }

}
