package org.yupana.jdbc

import java.sql.{Array => _, _}

import org.joda.time.LocalDateTime
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.yupana.api.Time
import org.yupana.api.query.SimpleResult
import org.yupana.api.types.DataType

class YupanaResultSetTest extends FlatSpec with Matchers with MockFactory {

  "Result set" should "provide common information" in {
    val statement = mock[Statement]
    val result = SimpleResult(
      "test",
      Seq("int", "string", "double"),
      Seq(DataType[Int], DataType[String], DataType[Double]),
      Iterator(
        Array[Option[Any]](Some(42), Some("foo"), None)
      )
    )

    val resultSet = new YupanaResultSet(statement, result)

    resultSet.getFetchDirection shouldEqual ResultSet.FETCH_FORWARD
    resultSet.getType shouldEqual ResultSet.TYPE_FORWARD_ONLY
    resultSet.getConcurrency shouldEqual ResultSet.CONCUR_READ_ONLY

    resultSet.findColumn("string") shouldEqual 2
    the[SQLException] thrownBy resultSet.findColumn("bzzz") should have message "Unknown column bzzz"
  }

  it should "move forward" in {
    val statement = mock[Statement]
    val result = SimpleResult(
      "test",
      Seq("int", "string"),
      Seq(DataType[Int], DataType[String]),
      Iterator(
        Array[Option[Any]](Some(1), Some("aaa")),
        Array[Option[Any]](Some(2), Some("bbb")),
        Array[Option[Any]](Some(3), Some("ccc")),
        Array[Option[Any]](Some(4), Some("ddd")),
        Array[Option[Any]](Some(5), Some("eee")),
        Array[Option[Any]](Some(6), Some("fff")),
        Array[Option[Any]](Some(7), Some("ggg"))
      )
    )

    val resultSet = new YupanaResultSet(statement, result)

    resultSet.isBeforeFirst shouldBe true
    resultSet.isFirst shouldBe false

    resultSet.next shouldBe true
    resultSet.isFirst shouldBe true
    resultSet.getRow shouldEqual 1
    resultSet.getInt(1) shouldEqual 1

    resultSet.relative(2) shouldBe true
    resultSet.getRow shouldEqual 3
    resultSet.getInt(1) shouldEqual 3

    an[SQLException] should be thrownBy resultSet.relative(-2)
    resultSet.getRow shouldEqual 3
    resultSet.getInt(1) shouldEqual 3

    resultSet.next shouldBe true
    resultSet.getRow shouldEqual 4
    resultSet.getInt(1) shouldEqual 4

    resultSet.absolute(6) shouldBe true
    resultSet.getRow shouldEqual 6
    resultSet.getInt(1) shouldEqual 6

    an[SQLException] should be thrownBy resultSet.absolute(5)
    resultSet.getRow shouldEqual 6
    resultSet.getInt(1) shouldEqual 6

    resultSet.last shouldBe true
    resultSet.getRow shouldEqual 7
    resultSet.getInt(1) shouldEqual 7

    resultSet.next shouldBe false
    resultSet.getRow shouldEqual 7
    resultSet.getInt(1) shouldEqual 7

  }

  it should "provide columns metadata" in {
    val statement = mock[Statement]
    val result = SimpleResult(
      "test",
      Seq("int", "string", "decimal", "time"),
      Seq(DataType[Int], DataType[String], DataType[BigDecimal], DataType[Time]),
      Iterator.empty
    )

    val resultSet = new YupanaResultSet(statement, result)
    val meta = resultSet.getMetaData

    meta.getColumnCount shouldEqual 4

    meta.isSigned(1) shouldBe true
    meta.isCurrency(1) shouldBe false
    meta.isNullable(1) shouldBe ResultSetMetaData.columnNullable
    meta.isCaseSensitive(1) shouldBe false
    meta.getColumnClassName(1) shouldEqual "java.lang.Integer"
    meta.getColumnType(1) shouldEqual Types.INTEGER
    meta.getCatalogName(1) shouldEqual ""
    meta.getPrecision(1) shouldEqual 10
    meta.getScale(1) shouldEqual 0

    meta.isSigned(2) shouldBe false
    meta.isCurrency(2) shouldBe false
    meta.isNullable(2) shouldBe ResultSetMetaData.columnNullable
    meta.isCaseSensitive(2) shouldBe false
    meta.getColumnClassName(2) shouldEqual "java.lang.String"
    meta.getColumnType(2) shouldEqual Types.VARCHAR
    meta.getTableName(2) shouldEqual "test"
    meta.getPrecision(2) shouldEqual Int.MaxValue

    meta.isSigned(3) shouldBe true
    meta.isCurrency(3) shouldBe false
    meta.isNullable(3) shouldBe ResultSetMetaData.columnNullable
    meta.isCaseSensitive(3) shouldBe false
    meta.getColumnClassName(3) shouldEqual "java.math.BigDecimal"
    meta.getColumnType(3) shouldEqual Types.DECIMAL
    meta.getTableName(3) shouldEqual "test"
    meta.getPrecision(3) shouldEqual 0
    meta.getScale(3) shouldEqual 0

    meta.isSigned(4) shouldBe false
    meta.isCurrency(4) shouldBe false
    meta.isNullable(4) shouldBe ResultSetMetaData.columnNullable
    meta.isCaseSensitive(4) shouldBe false
    meta.getColumnClassName(4) shouldEqual "java.sql.Timestamp"
    meta.getColumnType(4) shouldEqual Types.TIMESTAMP
    meta.getTableName(4) shouldEqual "test"
    meta.getPrecision(4) shouldEqual 23
    meta.getScale(4) shouldEqual 6
  }

  it should "extract data rows of different types" in {
    val statement = mock[Statement]
    val time = new LocalDateTime()
    val result = SimpleResult(
      "test",
      Seq("time", "bool", "int", "string", "double", "long"),
      Seq(DataType[Time], DataType[Boolean], DataType[Int], DataType[String], DataType[Double], DataType[Long]),
      Iterator(
        Array[Option[Any]](Some(Time(time)), Some(false), Some(42), Some("foo"), Some(55.5D), Some(10L)),
        Array[Option[Any]](None, None, None, None, None, None)
      )
    )

    val resultSet = new YupanaResultSet(statement, result)
    resultSet.next

    resultSet.getTimestamp(1) shouldEqual new Timestamp(time.toDateTime.getMillis)
    resultSet.getTimestamp("time") shouldEqual new Timestamp(time.toDateTime.getMillis)

    resultSet.getBoolean(2) shouldEqual false
    resultSet.getBoolean("bool") shouldEqual false

    resultSet.getInt(3) shouldEqual 42
    resultSet.getInt("int") shouldEqual 42
    resultSet.wasNull shouldBe false

    resultSet.getString(4) shouldEqual "foo"
    resultSet.getString("string") shouldEqual "foo"

    resultSet.getDouble(5) shouldEqual 55.5d
    resultSet.getDouble("double") shouldEqual 55.5d

    resultSet.getLong(6) shouldEqual 10L
    resultSet.getLong("long") shouldEqual 10L

    resultSet.next

    resultSet.getTimestamp(1) shouldBe null
    resultSet.getTimestamp("time") shouldBe null

    resultSet.getBoolean(2) shouldEqual false
    resultSet.getBoolean("bool") shouldEqual false

    resultSet.getInt(3) shouldEqual 0
    resultSet.getInt("int") shouldEqual 0
    resultSet.wasNull shouldBe true

    resultSet.getString(4) shouldEqual null
    resultSet.getString("string") shouldEqual null

    resultSet.getDouble(5) shouldEqual 0d
    resultSet.getDouble("double") shouldEqual 0d

    resultSet.getLong(6) shouldEqual 0L
    resultSet.getLong("long") shouldEqual 0L
  }
}
