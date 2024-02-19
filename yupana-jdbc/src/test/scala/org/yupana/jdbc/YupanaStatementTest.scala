package org.yupana.jdbc

import java.sql.{ ResultSet, SQLException, SQLFeatureNotSupportedException }
import org.scalamock.scalatest.MockFactory
import org.yupana.api.query.SimpleResult
import org.yupana.api.types.DataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.jdbc.YupanaConnection.QueryResult
import org.yupana.protocol.ParameterValue

class YupanaStatementTest extends AnyFlatSpec with Matchers with MockFactory {

  "YupanaStatement" should "execute queries" in {
    val conn = mock[YupanaConnection]
    val q = """SELECT item, kkmId FROM kkm_items
              |  WHERE time >= TIMESTAMP '2020-10-24' and time < TIMESTAMP '2020-10-30""".stripMargin

    val statement = new YupanaStatement(conn)

    val result = SimpleResult(
      "result",
      Seq("item", "kkm_id"),
      Seq(DataType[String], DataType[Int]),
      Iterator(Array[Any]("thing", 1), Array[Any]("Another", 4))
    )

    (conn.runQuery _).expects(q, Map.empty[Int, ParameterValue]).returning(QueryResult(1, result))

    statement.execute(q) shouldBe true
    val rs = statement.getResultSet
    rs.next()
    rs.getString("item") shouldEqual "thing"

    rs.isClosed shouldBe false

    (conn.cancelStream _).expects(1).once()
    statement.close()

    rs.isClosed shouldBe true
    statement.isClosed shouldBe true
  }

  it should "provide statement info or indicate not supported features" in {
    val conn = mock[YupanaConnection]
    val statement = new YupanaStatement(conn)

    statement.getWarnings shouldBe null
    statement.clearWarnings()
    statement.getWarnings shouldBe null

    statement.getConnection shouldEqual conn

    an[SQLFeatureNotSupportedException] should be thrownBy statement.closeOnCompletion()
    statement.isCloseOnCompletion shouldBe false

    statement.getResultSetType shouldEqual ResultSet.TYPE_FORWARD_ONLY
    statement.getResultSetConcurrency shouldEqual ResultSet.CONCUR_READ_ONLY
    statement.getResultSetHoldability shouldEqual ResultSet.CLOSE_CURSORS_AT_COMMIT
    statement.getFetchDirection shouldEqual ResultSet.FETCH_FORWARD
    an[SQLException] should be thrownBy statement.setFetchDirection(ResultSet.FETCH_REVERSE)

    an[SQLFeatureNotSupportedException] should be thrownBy statement.getMoreResults(1)
    an[SQLFeatureNotSupportedException] should be thrownBy statement.getGeneratedKeys

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setMaxFieldSize(1234)
    an[SQLFeatureNotSupportedException] should be thrownBy statement.getMaxFieldSize

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setQueryTimeout(60)
    statement.getQueryTimeout shouldEqual 0

    an[SQLFeatureNotSupportedException] should be thrownBy statement.executeUpdate("UPSERT (1) INTO items(kkmId)")

    an[SQLFeatureNotSupportedException] should be thrownBy statement.executeUpdate("UPSERT (1) INTO items(kkmId)", 2)
    an[SQLFeatureNotSupportedException] should be thrownBy statement.executeUpdate(
      "UPSERT (1) INTO items(kkmId)",
      Array(1, 2, 3)
    )
    an[SQLFeatureNotSupportedException] should be thrownBy statement.executeUpdate(
      "UPSERT (1) INTO items(kkmId)",
      Array("foo", "bar")
    )
    statement.getUpdateCount shouldEqual -1

    an[SQLFeatureNotSupportedException] should be thrownBy statement.execute(
      "UPSERT (1) INTO items(kkmId)",
      Array("aaa")
    )
    an[SQLFeatureNotSupportedException] should be thrownBy statement.execute("UPSERT (1) INTO items(kkmId)", 123)
    an[SQLFeatureNotSupportedException] should be thrownBy statement.execute(
      "UPSERT (1) INTO items(kkmId)",
      Array(1, 2)
    )

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setEscapeProcessing(true)

    statement.isPoolable shouldBe false
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setPoolable(true)

    an[SQLFeatureNotSupportedException] should be thrownBy statement.addBatch("SELECT a FROM b")
    an[SQLFeatureNotSupportedException] should be thrownBy statement.clearBatch()
    an[SQLFeatureNotSupportedException] should be thrownBy statement.executeBatch()
  }
}
