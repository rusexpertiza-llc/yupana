package org.yupana.jdbc

import java.sql.{ ResultSet, ResultSetMetaData, Statement, Types }

import org.scalamock.scalatest.MockFactory
import org.scalatest.{ FlatSpec, Matchers }
import org.yupana.api.Time
import org.yupana.api.query.SimpleResult
import org.yupana.api.types.DataType

class YupanaResultSetTest extends FlatSpec with Matchers with MockFactory {

  "Result set" should "move forward" in {
    val statement = mock[Statement]
    val result = SimpleResult(
      "test",
      Seq("int", "string"),
      Seq(DataType[Int], DataType[String]),
      Seq(
        Array[Option[Any]](Some(1), Some("foo")),
        Array[Option[Any]](None, Some("bar")),
        Array[Option[Any]](Some(42), Some("baz"))
      ).iterator
    )

    val resultSet = new YupanaResultSet(statement, result)

    resultSet.getFetchDirection shouldEqual ResultSet.FETCH_FORWARD

    resultSet.isBeforeFirst shouldBe true
    resultSet.next shouldBe true
    resultSet.getRow shouldEqual 1

    resultSet.next shouldBe true
    resultSet.getRow shouldEqual 2

    resultSet.next shouldBe true
    resultSet.getRow shouldEqual 3

    resultSet.next shouldBe false
    resultSet.getRow shouldEqual 3

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
}
