package org.yupana.jdbc

import java.sql.{ Array => _, _ }

import org.joda.time.LocalDateTime
import org.scalamock.scalatest.MockFactory
import org.scalatest.{ FlatSpec, Matchers }
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

    resultSet.getStatement shouldEqual statement

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
    resultSet.isLast shouldBe false
    resultSet.isAfterLast shouldBe false
    an[SQLException] should be thrownBy resultSet.getInt(1)

    resultSet.next shouldBe true
    resultSet.isBeforeFirst shouldBe false
    resultSet.isFirst shouldBe true
    resultSet.isLast shouldBe false
    resultSet.isAfterLast shouldBe false
    resultSet.getRow shouldEqual 1
    resultSet.getInt(1) shouldEqual 1

    resultSet.relative(2) shouldBe true
    resultSet.isBeforeFirst shouldBe false
    resultSet.isFirst shouldBe false
    resultSet.isLast shouldBe false
    resultSet.isAfterLast shouldBe false
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
    resultSet.isBeforeFirst shouldBe false
    resultSet.isFirst shouldBe false
    resultSet.isLast shouldBe true
    resultSet.isAfterLast shouldBe false
    resultSet.getRow shouldEqual 7
    resultSet.getInt(1) shouldEqual 7

    resultSet.next shouldBe false
    resultSet.isBeforeFirst shouldBe false
    resultSet.isFirst shouldBe false
    resultSet.isLast shouldBe false
    resultSet.isAfterLast shouldBe true
    resultSet.getRow shouldEqual 7
    an[SQLException] should be thrownBy resultSet.getInt(1)
  }

  it should "provide columns metadata" in {
    val statement = mock[Statement]
    val result = SimpleResult(
      "test",
      Seq("age", "name", "salary", "birthday"),
      Seq(DataType[Int], DataType[String], DataType[BigDecimal], DataType[Time]),
      Iterator.empty
    )

    val resultSet = new YupanaResultSet(statement, result)
    val meta = resultSet.getMetaData

    meta.getColumnCount shouldEqual 4

    meta.getColumnName(1) shouldEqual "age"
    meta.getColumnLabel(1) shouldEqual "age"
    meta.isSigned(1) shouldBe true
    meta.isCurrency(1) shouldBe false
    meta.isNullable(1) shouldBe ResultSetMetaData.columnNullable
    meta.isCaseSensitive(1) shouldBe false
    meta.getColumnClassName(1) shouldEqual "java.lang.Integer"
    meta.getColumnTypeName(1) shouldEqual "INTEGER"
    meta.getColumnType(1) shouldEqual Types.INTEGER
    meta.getCatalogName(1) shouldEqual ""
    meta.getSchemaName(1) shouldEqual "test"
    meta.getTableName(1) shouldEqual "test"
    meta.getPrecision(1) shouldEqual 10
    meta.getColumnDisplaySize(1) shouldEqual 10
    meta.getScale(1) shouldEqual 0
    meta.isSearchable(1) shouldBe true
    meta.isReadOnly(1) shouldBe true
    meta.isAutoIncrement(1) shouldBe false
    meta.isWritable(1) shouldBe false
    meta.isDefinitelyWritable(1) shouldBe false

    meta.getColumnName(2) shouldEqual "name"
    meta.getColumnLabel(2) shouldEqual "name"
    meta.isSigned(2) shouldBe false
    meta.isCurrency(2) shouldBe false
    meta.isNullable(2) shouldBe ResultSetMetaData.columnNullable
    meta.isCaseSensitive(2) shouldBe false
    meta.getColumnClassName(2) shouldEqual "java.lang.String"
    meta.getColumnTypeName(2) shouldEqual "VARCHAR"
    meta.getColumnType(2) shouldEqual Types.VARCHAR
    meta.getTableName(2) shouldEqual "test"
    meta.getSchemaName(2) shouldEqual "test"
    meta.getPrecision(2) shouldEqual Int.MaxValue
    meta.getColumnDisplaySize(2) shouldEqual Int.MaxValue
    meta.isSearchable(2) shouldBe true
    meta.isReadOnly(2) shouldBe true
    meta.isAutoIncrement(2) shouldBe false
    meta.isWritable(2) shouldBe false
    meta.isDefinitelyWritable(2) shouldBe false

    meta.getColumnName(3) shouldEqual "salary"
    meta.getColumnLabel(3) shouldEqual "salary"
    meta.isSigned(3) shouldBe true
    meta.isCurrency(3) shouldBe false
    meta.isNullable(3) shouldBe ResultSetMetaData.columnNullable
    meta.isCaseSensitive(3) shouldBe false
    meta.getColumnClassName(3) shouldEqual "java.math.BigDecimal"
    meta.getColumnTypeName(3) shouldEqual "DECIMAL"
    meta.getColumnType(3) shouldEqual Types.DECIMAL
    meta.getTableName(3) shouldEqual "test"
    meta.getSchemaName(3) shouldEqual "test"
    meta.getPrecision(3) shouldEqual 0
    meta.getScale(3) shouldEqual 0
    meta.getColumnDisplaySize(3) shouldEqual 131089
    meta.isSearchable(3) shouldBe true
    meta.isReadOnly(3) shouldBe true
    meta.isAutoIncrement(3) shouldBe false
    meta.isWritable(3) shouldBe false
    meta.isDefinitelyWritable(3) shouldBe false

    meta.getColumnName(4) shouldEqual "birthday"
    meta.getColumnLabel(4) shouldEqual "birthday"
    meta.isSigned(4) shouldBe false
    meta.isCurrency(4) shouldBe false
    meta.isNullable(4) shouldBe ResultSetMetaData.columnNullable
    meta.isCaseSensitive(4) shouldBe false
    meta.getColumnClassName(4) shouldEqual "java.sql.Timestamp"
    meta.getColumnTypeName(4) shouldEqual "TIMESTAMP"
    meta.getColumnType(4) shouldEqual Types.TIMESTAMP
    meta.getTableName(4) shouldEqual "test"
    meta.getSchemaName(4) shouldEqual "test"
    meta.getPrecision(4) shouldEqual 23
    meta.getScale(4) shouldEqual 6
    meta.getColumnDisplaySize(4) shouldEqual 23
    meta.isSearchable(4) shouldBe true
    meta.isReadOnly(4) shouldBe true
    meta.isAutoIncrement(4) shouldBe false
    meta.isWritable(4) shouldBe false
    meta.isDefinitelyWritable(4) shouldBe false
  }

  it should "extract data rows of different types" in {
    val statement = mock[Statement]
    val time = new LocalDateTime()
    val result = SimpleResult(
      "test",
      Seq("time", "bool", "int", "string", "double", "long", "decimal"),
      Seq(
        DataType[Time],
        DataType[Boolean],
        DataType[Int],
        DataType[String],
        DataType[Double],
        DataType[Long],
        DataType[BigDecimal]
      ),
      Iterator(
        Array[Option[Any]](
          Some(Time(time)),
          Some(false),
          Some(42),
          Some("foo"),
          Some(55.5d),
          Some(10L),
          Some(BigDecimal(1234.321))
        ),
        Array[Option[Any]](None, None, None, None, None, None)
      )
    )

    val resultSet = new YupanaResultSet(statement, result)
    resultSet.next

    resultSet.getTimestamp(1) shouldEqual new Timestamp(time.toDateTime.getMillis)
    resultSet.getTimestamp("time") shouldEqual new Timestamp(time.toDateTime.getMillis)

    resultSet.getDate(1) shouldEqual new Date(time.toDateTime.getMillis)
    resultSet.getDate("time") shouldEqual new Date(time.toDateTime.getMillis)

    resultSet.getTime(1) shouldEqual new java.sql.Time(time.toDateTime.getMillis)
    resultSet.getTime("time") shouldEqual new java.sql.Time(time.toDateTime.getMillis)

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

    resultSet.getBigDecimal(7) shouldEqual java.math.BigDecimal.valueOf(1234.321)
    resultSet.getBigDecimal("decimal") shouldEqual java.math.BigDecimal.valueOf(1234.321)

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
