package org.yupana.externallinks.universal

import java.sql.{ Connection, PreparedStatement, ResultSet }
import java.util.Properties
import javax.sql.DataSource
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.core.cache.CacheFactory
import org.yupana.externallinks.universal.JsonCatalogs.{
  SQLExternalLink,
  SQLExternalLinkConfig,
  SQLExternalLinkConnection,
  SQLExternalLinkDescription
}
import org.yupana.schema.{ Dimensions, SchemaRegistry }

class JsonExternalLinkCachingTest extends AnyFlatSpec with Matchers with MockFactory with BeforeAndAfterAll {

  import JsonExternalLinkCachingTest._

  override def beforeAll(): Unit = {
    val props = new Properties()
    props.put("analytics.caches.default.engine", "EhCache")
    props.put("analytics.caches.TestLink_fields.maxElements", "100")
    props.put("analytics.caches.TestLink_fields.heapSize", "1024")
    CacheFactory.init(props, "ns")
  }

  override def afterAll(): Unit = {
    resetCacheFactory()
  }

  "SQL sourced universal external link" should "use cache" in {

    val linkDesc = SQLExternalLinkDescription("TestLink", "kkmId", Set("f1", "f2"), Seq("items_kkm"), None, None)
    val linkConn = SQLExternalLinkConnection("jdbc:postgresql://host:5432/db", Some("root"), Some("root"))

    val linkConfig = SQLExternalLinkConfig(linkDesc, linkConn)

    val linkSpec = SQLExternalLink(linkConfig, Dimensions.KKM_ID)

    val dataSource = mock[DataSource]
    val conn = mock[Connection]
    val statement = mock[PreparedStatement]
    val resultSet = mock[ResultSet]

    (() => dataSource.getConnection).expects().returning(conn)
    (conn.prepareStatement(_: String)).expects(*).returning(statement)

    (statement.setObject(_: Int, _: Any)).expects(1, Integer.valueOf(578941516))
    (() => statement.executeQuery).expects().returning(resultSet)

    (resultSet.next _).expects().returning(true)
    (resultSet.findColumn _).expects("f1").returning(1)
    (resultSet.getObject(_: Int)).expects(1).returning("f1Value")
    (resultSet.findColumn _).expects("kkm_id").returning(2)
    (resultSet.getObject(_: Int)).expects(2).returning(Integer.valueOf(578941516))

    (resultSet.next _).expects().returning(false)
    (statement.close _).expects()
    (conn.close _).expects()

    val link = new SQLSourcedExternalLinkService(SchemaRegistry.defaultSchema, linkSpec, linkDesc, dataSource)

    val r1 = link.fieldValuesForDimValues(Set("f1"), Set(578941516))
    val r2 = link.fieldValuesForDimValues(Set("f1"), Set(578941516))

    r1 shouldEqual r2
  }
}

object JsonExternalLinkCachingTest {

  def resetCacheFactory(): Unit = {
    val propsField = CacheFactory.getClass.getDeclaredField("properties")
    propsField.setAccessible(true)
    propsField.set(CacheFactory, null)
    println("dropped props")
  }
}
