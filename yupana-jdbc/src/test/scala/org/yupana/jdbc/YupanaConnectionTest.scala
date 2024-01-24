package org.yupana.jdbc

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.query.{ Result, SimpleResult }
import org.yupana.api.types.DataType
import org.yupana.jdbc.YupanaConnection.QueryResult
import org.yupana.protocol.ParameterValue

import java.sql.{ Connection, ResultSet, SQLClientInfoException, SQLFeatureNotSupportedException, Statement }
import java.util.Properties
import java.util.concurrent.ForkJoinPool

class YupanaConnectionTest extends AnyFlatSpec with Matchers {

  class TestConnection extends YupanaConnection {

    private var closed = false
    override def runQuery(query: String, params: Map[Int, ParameterValue]): QueryResult = {
      if (query == "SELECT 1") {
        QueryResult(1, SimpleResult("test", Seq("1"), Seq(DataType[BigDecimal]), Iterator(Array[Any](BigDecimal(1)))))
      } else {
        QueryResult(1, Result.empty)
      }
    }

    override def runBatchQuery(query: String, params: Seq[Map[Int, ParameterValue]]): QueryResult =
      QueryResult(1, Result.empty)

    override def url: String = ""

    override def close(): Unit = closed = true

    override def isClosed: Boolean = closed

    override def cancelStream(streamId: Int): Unit = {}
  }

  "YupanaConnection" should "provide connection capabilities" in {
    val c = new TestConnection

    c.setCatalog("aaa")
    c.getCatalog shouldBe null
    c.setSchema("bbb")
    c.getSchema shouldBe null
    c.getWarnings shouldBe null
    c.clearWarnings()
    c.getWarnings shouldBe null
    c.getAutoCommit shouldBe true
    c.setAutoCommit(false)
    c.getAutoCommit shouldBe false
    c.setAutoCommit(true)
    c.getAutoCommit shouldBe true
    c.getTransactionIsolation shouldEqual Connection.TRANSACTION_NONE
    a[SQLFeatureNotSupportedException] should be thrownBy c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)
    c.isValid(10) shouldBe true

    c.getHoldability shouldEqual ResultSet.HOLD_CURSORS_OVER_COMMIT
    a[SQLFeatureNotSupportedException] should be thrownBy c.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT)

    a[SQLClientInfoException] should be thrownBy c.setClientInfo("param", "value")
    a[SQLClientInfoException] should be thrownBy c.setClientInfo(new Properties())
    a[SQLClientInfoException] should be thrownBy c.getClientInfo("key")
    a[SQLClientInfoException] should be thrownBy c.getClientInfo()

    a[SQLFeatureNotSupportedException] should be thrownBy c.setNetworkTimeout(ForkJoinPool.commonPool(), 33)
    a[SQLFeatureNotSupportedException] should be thrownBy c.getNetworkTimeout
  }

  it should "create JDBC entities" in {
    val c = new TestConnection

    val st = c.createStatement
    st.getConnection shouldEqual c

    val ps = c.prepareStatement("SELECT ?")
    ps.getConnection shouldEqual c

    a[SQLFeatureNotSupportedException] should be thrownBy c.createStatement(
      ResultSet.TYPE_SCROLL_INSENSITIVE,
      ResultSet.CONCUR_READ_ONLY,
      ResultSet.HOLD_CURSORS_OVER_COMMIT
    )

    a[SQLFeatureNotSupportedException] should be thrownBy c.prepareStatement(
      "SELECT 1",
      Statement.RETURN_GENERATED_KEYS
    )

    a[SQLFeatureNotSupportedException] should be thrownBy c.prepareStatement(
      "SELECT 2",
      ResultSet.TYPE_SCROLL_INSENSITIVE,
      ResultSet.CONCUR_READ_ONLY,
      ResultSet.HOLD_CURSORS_OVER_COMMIT
    )

    a[SQLFeatureNotSupportedException] should be thrownBy c.prepareStatement(
      "SELECT 1",
      Array(1)
    )

    a[SQLFeatureNotSupportedException] should be thrownBy c.prepareStatement(
      "SELECT 1",
      Array("one")
    )

    a[SQLFeatureNotSupportedException] should be thrownBy c.createBlob
    a[SQLFeatureNotSupportedException] should be thrownBy c.createClob
    a[SQLFeatureNotSupportedException] should be thrownBy c.createNClob
    a[SQLFeatureNotSupportedException] should be thrownBy c.createSQLXML
    a[SQLFeatureNotSupportedException] should be thrownBy c.createStruct("test", Array())

    a[SQLFeatureNotSupportedException] should be thrownBy c.prepareCall("{call function()}")
    a[SQLFeatureNotSupportedException] should be thrownBy c.prepareCall(
      "{call function()}",
      ResultSet.CONCUR_READ_ONLY,
      ResultSet.HOLD_CURSORS_OVER_COMMIT
    )

    a[SQLFeatureNotSupportedException] should be thrownBy c.setSavepoint()
    a[SQLFeatureNotSupportedException] should be thrownBy c.setSavepoint("point 1")
    a[SQLFeatureNotSupportedException] should be thrownBy c.abort(ForkJoinPool.commonPool())
  }

}
