package org.yupana.jdbc

import java.io.{ ByteArrayInputStream, CharArrayReader }
import java.sql.{ Array => _, _ }
import java.util.{ Calendar, Scanner, TimeZone }
import java.{ util, math => jm }

import org.joda.time.{ DateTimeZone, LocalDateTime }
import org.scalamock.scalatest.MockFactory
import org.yupana.api.Time
import org.yupana.api.query.SimpleResult
import org.yupana.api.types.{ DataType, DataTypeMeta }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class YupanaResultSetTest extends AnyFlatSpec with Matchers with MockFactory {

  "Result set" should "provide common information" in {
    val statement = mock[Statement]
    val result = SimpleResult(
      "test",
      Seq("int", "string", "double"),
      Seq(DataType[Int], DataType[String], DataType[Double]),
      Iterator(
        Array[Any](42, "foo", null)
      )
    )

    val resultSet = new YupanaResultSet(statement, result)

    resultSet.getFetchDirection shouldEqual ResultSet.FETCH_FORWARD
    resultSet.getType shouldEqual ResultSet.TYPE_FORWARD_ONLY
    resultSet.getConcurrency shouldEqual ResultSet.CONCUR_READ_ONLY
    resultSet.getHoldability shouldEqual ResultSet.HOLD_CURSORS_OVER_COMMIT

    an[SQLException] should be thrownBy resultSet.setFetchDirection(ResultSet.FETCH_REVERSE)

    resultSet.setFetchDirection(ResultSet.FETCH_FORWARD)
    resultSet.getFetchDirection shouldEqual ResultSet.FETCH_FORWARD

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
        Array[Any](1, "aaa"),
        Array[Any](2, "bbb"),
        Array[Any](3, "ccc"),
        Array[Any](4, "ddd"),
        Array[Any](5, "eee"),
        Array[Any](6, "fff"),
        Array[Any](7, "ggg")
      )
    )

    val resultSet = new YupanaResultSet(statement, result)

    resultSet.isBeforeFirst shouldBe true
    resultSet.isFirst shouldBe false
    resultSet.isLast shouldBe false
    resultSet.isAfterLast shouldBe false
    an[SQLException] should be thrownBy resultSet.getInt(1)

    resultSet.beforeFirst()
    resultSet.isBeforeFirst shouldBe true

    resultSet.first shouldBe true
    resultSet.isBeforeFirst shouldBe false
    resultSet.isFirst shouldBe true
    resultSet.isLast shouldBe false
    resultSet.isAfterLast shouldBe false
    resultSet.getRow shouldEqual 1
    resultSet.getInt(1) shouldEqual 1

    resultSet.first shouldBe true
    resultSet.isBeforeFirst shouldBe false
    resultSet.isFirst shouldBe true

    an[SQLException] should be thrownBy resultSet.beforeFirst()

    resultSet.relative(2) shouldBe true
    resultSet.isBeforeFirst shouldBe false
    resultSet.isFirst shouldBe false
    resultSet.isLast shouldBe false
    resultSet.isAfterLast shouldBe false
    resultSet.getRow shouldEqual 3
    resultSet.getInt(1) shouldEqual 3

    an[SQLException] should be thrownBy resultSet.first()
    an[SQLException] should be thrownBy resultSet.previous()

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
    an[SQLException] should be thrownBy resultSet.last
  }

  it should "support last and afterLast" in {
    val statement = mock[Statement]
    val result = SimpleResult(
      "test",
      Seq("int", "string"),
      Seq(DataType[Int], DataType[String]),
      Iterator(
        Array[Any](1, "aaa"),
        Array[Any](2, "bbb")
      )
    )

    val resultSet = new YupanaResultSet(statement, result)

    resultSet.last()
    resultSet.isBeforeFirst shouldBe false
    resultSet.isFirst shouldBe false
    resultSet.isLast shouldBe true
    resultSet.isAfterLast shouldBe false
    resultSet.getRow shouldEqual 2
    resultSet.getInt(1) shouldEqual 2

    resultSet.afterLast()
    resultSet.isBeforeFirst shouldBe false
    resultSet.isFirst shouldBe false
    resultSet.isLast shouldBe false
    resultSet.isAfterLast shouldBe true
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
    meta.getScale(3) shouldEqual DataTypeMeta.MONEY_SCALE
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
        DataType[BigDecimal],
        DataType[Long],
        DataType[BigDecimal]
      ),
      Iterator(
        Array[Any](
          Time(time),
          false,
          42,
          "foo",
          55.5d,
          10L,
          BigDecimal(1234.321)
        ),
        Array[Any](null, null, null, null, null, null)
      )
    )

    val resultSet = new YupanaResultSet(statement, result)
    resultSet.next

    resultSet.getTimestamp(1) shouldEqual new Timestamp(time.toDateTime.getMillis)
    resultSet.getTimestamp("time") shouldEqual new Timestamp(time.toDateTime.getMillis)
    resultSet.getTimestamp(1, Calendar.getInstance()) shouldEqual new Timestamp(
      time.toDateTime(DateTimeZone.getDefault).getMillis
    )
    resultSet.getTimestamp(
      "time",
      Calendar.getInstance(TimeZone.getTimeZone("Europe/Helsinki"))
    ) shouldEqual new Timestamp(
      time.toDateTime(DateTimeZone.forID("Europe/Helsinki")).getMillis
    )

    resultSet.getDate(1) shouldEqual new Date(time.toDateTime.getMillis)
    resultSet.getDate("time") shouldEqual new Date(time.toDateTime.getMillis)
    resultSet.getDate(1, Calendar.getInstance()) shouldEqual new Date(
      time.toDateTime(DateTimeZone.getDefault).getMillis
    )
    resultSet.getDate("time", Calendar.getInstance(TimeZone.getTimeZone("Pacific/Honolulu"))) shouldEqual new Date(
      time.toDateTime(DateTimeZone.forID("Pacific/Honolulu")).getMillis
    )

    resultSet.getTime(1) shouldEqual new java.sql.Time(time.toDateTime.getMillis)
    resultSet.getTime("time") shouldEqual new java.sql.Time(time.toDateTime.getMillis)
    resultSet.getTime(1, Calendar.getInstance(TimeZone.getTimeZone("Africa/Lome"))) shouldEqual new java.sql.Time(
      time.toDateTime(DateTimeZone.forID("Africa/Lome")).getMillis
    )
    resultSet.getTime("time", Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"))) shouldEqual new java.sql.Time(
      time.toDateTime(DateTimeZone.forID("Asia/Tokyo")).getMillis
    )

    resultSet.getBoolean(2) shouldEqual false
    resultSet.getBoolean("bool") shouldEqual false

    resultSet.getInt(3) shouldEqual 42
    resultSet.getInt("int") shouldEqual 42
    resultSet.wasNull shouldBe false

    resultSet.getString(4) shouldEqual "foo"
    resultSet.getString("string") shouldEqual "foo"
    new Scanner(resultSet.getAsciiStream(4)).next() shouldEqual "foo"
    new Scanner(resultSet.getAsciiStream("string")).next() shouldEqual "foo"
    new Scanner(resultSet.getUnicodeStream(4)).next() shouldEqual "foo"
    new Scanner(resultSet.getUnicodeStream("string")).next() shouldEqual "foo"
    new Scanner(resultSet.getCharacterStream(4)).next() shouldEqual "foo"
    new Scanner(resultSet.getCharacterStream("string")).next() shouldEqual "foo"
    resultSet.getObject(4) shouldEqual "foo"
    resultSet.getObject("string") shouldEqual "foo"

    resultSet.getDouble(5) shouldEqual 55.5d
    resultSet.getDouble("double") shouldEqual 55.5d

    resultSet.getLong(6) shouldEqual 10L
    resultSet.getLong("long") shouldEqual 10L

    resultSet.getBigDecimal(7) shouldEqual jm.BigDecimal.valueOf(1234.321)
    resultSet.getBigDecimal("decimal") shouldEqual jm.BigDecimal.valueOf(1234.321)
    resultSet.getBigDecimal(7, 6) shouldEqual jm.BigDecimal.valueOf(1234.321).setScale(6)
    an[ArithmeticException] should be thrownBy resultSet.getBigDecimal("decimal", 1)

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

  it should "provide correct info about null values" in {
    val statement = mock[Statement]
    val time = LocalDateTime.now()

    val result = SimpleResult(
      "test",
      Seq("int", "string", "double", "time"),
      Seq(DataType[Int], DataType[String], DataType[Double], DataType[Time]),
      Iterator(
        Array[Any](42, null, null, Time(time)),
        Array[Any](null, "not null", 88d, null)
      )
    )

    val resultSet = new YupanaResultSet(statement, result)

    resultSet.next
    resultSet.getInt(1) shouldEqual 42
    resultSet.wasNull shouldBe false

    resultSet.getString(2) shouldEqual null
    resultSet.wasNull shouldBe true

    resultSet.getDouble(3) shouldEqual 0d
    resultSet.wasNull shouldBe true

    resultSet.getTimestamp(4) shouldEqual new Timestamp(time.toDateTime.getMillis)
    resultSet.wasNull shouldBe false

    resultSet.next
    resultSet.getInt(1) shouldEqual 0
    resultSet.wasNull shouldBe true

    resultSet.getString(2) shouldEqual "not null"
    resultSet.wasNull shouldBe false

    resultSet.getDouble(3) shouldEqual 88d
    resultSet.wasNull shouldBe false

    resultSet.getTimestamp(4) shouldEqual null
    resultSet.wasNull shouldBe true
  }

  it should "support closing" in {
    val resultSet = createResultSet

    resultSet.isClosed shouldBe false
    resultSet.next
    resultSet.close()
    resultSet.isClosed shouldBe true

    the[SQLException] thrownBy resultSet.isFirst should have message "ResultSet is already closed"
    the[SQLException] thrownBy resultSet.getInt(1) should have message "ResultSet is already closed"
  }

  it should "not support fetch size" in {
    val rs = createResultSet
    rs.getFetchSize shouldEqual 0
    an[SQLFeatureNotSupportedException] should be thrownBy rs.setFetchSize(10)
  }

  it should "not support cursors" in {
    an[SQLFeatureNotSupportedException] should be thrownBy createResultSet.getCursorName
  }

  it should "throw exception on unsupported getters" in {
    val rs = createResultSet
    rs.next

    an[SQLFeatureNotSupportedException] should be thrownBy rs.getClob(1)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.getClob("string")

    an[SQLFeatureNotSupportedException] should be thrownBy rs.getNClob(1)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.getNClob("string")

    an[SQLFeatureNotSupportedException] should be thrownBy rs.getNCharacterStream(1)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.getNCharacterStream("string")

    an[SQLFeatureNotSupportedException] should be thrownBy rs.getNString(1)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.getNString("string")

    an[SQLFeatureNotSupportedException] should be thrownBy rs.getURL(1)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.getURL("string")

    an[SQLFeatureNotSupportedException] should be thrownBy rs.getSQLXML(1)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.getSQLXML("string")

    an[SQLFeatureNotSupportedException] should be thrownBy rs.getRef(1)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.getRef("string")

    an[SQLFeatureNotSupportedException] should be thrownBy rs.getRowId(1)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.getRowId("string")
  }

  it should "support arrays" in {
    val statement = mock[Statement]
    val result = SimpleResult(
      "test",
      Seq("int", "array_string", "array_int"),
      Seq(DataType[Int], DataType[Seq[String]], DataType[Seq[Int]]),
      Iterator(Array[Any](42, Seq("Foo", "bar"), Seq(1, 2, 4, 8)))
    )

    val rs = new YupanaResultSet(statement, result)
    rs.next

    the[SQLException] thrownBy rs.getArray(1) should have message "INTEGER is not an array"

    val stringArray = rs.getArray(2)
    stringArray.getBaseType shouldEqual Types.VARCHAR
    stringArray.getBaseTypeName shouldEqual "VARCHAR"
    stringArray.getArray shouldEqual Array("Foo", "bar")
    val stringRs = stringArray.getResultSet()
    stringRs.next()
    stringRs.getInt("INDEX").shouldEqual(1)
    stringRs.getString("VALUE").shouldEqual("Foo")
    stringRs.next()
    stringRs.getInt("INDEX").shouldEqual(2)
    stringRs.getString("VALUE").shouldEqual("bar")
    stringRs.next() shouldBe false

    val intArray = rs.getArray("array_int")
    intArray.getBaseType shouldEqual Types.INTEGER
    intArray.getBaseTypeName shouldEqual "INTEGER"
    intArray.getArray(2, 2) shouldEqual Array(2, 4)
    val intRs = intArray.getResultSet(3, 1, new java.util.HashMap())
    intRs.next()
    intRs.getInt(1) shouldEqual (3)
    intRs.getInt(2) shouldEqual (4)
    intRs.next() shouldBe false

    val mapping = new util.HashMap[String, Class[_]]()
    mapping.put("SHORT", classOf[java.lang.Integer])
    an[SQLFeatureNotSupportedException] should be thrownBy stringArray.getArray(mapping)
    an[SQLFeatureNotSupportedException] should be thrownBy stringArray.getArray(1, 2, mapping)
    an[SQLFeatureNotSupportedException] should be thrownBy stringArray.getResultSet(1, 2, mapping)
    an[SQLFeatureNotSupportedException] should be thrownBy intArray.getResultSet(mapping)

  }

  it should "support BLOBs" in {
    import org.yupana.api.{ Blob => ApiBlob }

    val statement = mock[Statement]

    val result = SimpleResult(
      "test",
      Seq("int", "bytes"),
      Seq(DataType[Int], DataType[ApiBlob]),
      Iterator(Array[Any](42, ApiBlob(Array[Byte](2, 12, 85, 0, 6))))
    )

    val rs = new YupanaResultSet(statement, result)
    rs.next

    val blob = rs.getBlob(2)
    blob.length() shouldEqual 5
    an[SQLFeatureNotSupportedException] should be thrownBy blob.setBytes(2, Array(3, 4))
    an[SQLFeatureNotSupportedException] should be thrownBy blob.setBytes(1, Array(3, 4, 5, 6, 7), 3, 2)
    an[SQLFeatureNotSupportedException] should be thrownBy blob.setBinaryStream(3)
    an[SQLFeatureNotSupportedException] should be thrownBy blob.truncate(3)

    val blob2 = rs.getBlob("bytes")
    blob2.getBytes(2, 2) should contain theSameElementsInOrderAs Seq[Byte](12, 85)

    val bytes = new Array[Byte](5)
    blob2.getBinaryStream.read(bytes)
    bytes should contain theSameElementsInOrderAs Seq[Byte](2, 12, 85, 0, 6)

    val bytes2 = new Array[Byte](3)
    blob2.getBinaryStream(2, 3).read(bytes2)
    bytes2 should contain theSameElementsInOrderAs Seq[Byte](12, 85, 0)
  }

  it should "throw exception on update operation" in {
    val rs = createResultSet

    an[SQLFeatureNotSupportedException] should be thrownBy rs.insertRow()
    an[SQLFeatureNotSupportedException] should be thrownBy rs.moveToInsertRow()
    an[SQLFeatureNotSupportedException] should be thrownBy rs.moveToCurrentRow()

    an[SQLFeatureNotSupportedException] should be thrownBy rs.deleteRow()
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateRow()
    an[SQLFeatureNotSupportedException] should be thrownBy rs.refreshRow()
    an[SQLFeatureNotSupportedException] should be thrownBy rs.cancelRowUpdates()

    an[SQLFeatureNotSupportedException] should be thrownBy rs.rowInserted()
    an[SQLFeatureNotSupportedException] should be thrownBy rs.rowUpdated()
    an[SQLFeatureNotSupportedException] should be thrownBy rs.rowDeleted()

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateString(2, "bar")
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateString("string", "value")
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateNString(2, "bar")
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateNString("string", "value")

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateBigDecimal(1, jm.BigDecimal.ONE)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateBigDecimal("int", jm.BigDecimal.TEN)

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateInt(1, 1)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateInt("int", 2)

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateLong(1, 1)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateLong("int", 2)

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateShort(1, 1: Short)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateShort("int", 2: Short)

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateByte(1, 1: Byte)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateByte("int", 2: Byte)

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateDouble(3, 1d)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateDouble("double", 2d)

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateFloat(3, 1f)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateFloat("double", 2f)

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateTimestamp(4, new Timestamp(123456L))
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateTimestamp("time", new Timestamp(654321L))

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateDate(4, new Date(123456L))
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateDate("time", new Date(654321L))

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateTime(4, new java.sql.Time(123456L))
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateTime("time", new java.sql.Time(654321L))

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateBoolean(4, true)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateBoolean("time", false)

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateNull(1)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateNull("string")

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateObject(1, 41)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateObject(1, 41, 5)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateObject("string", "42")
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateObject("string", "42", 3)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateObject("string", "42", JDBCType.VARCHAR)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateObject("string", "42", JDBCType.INTEGER)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateObject("string", "42", JDBCType.VARCHAR, 5)
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateObject("string", "42", JDBCType.INTEGER, 2)

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateBytes(1, Array[Byte](1, 2, 3))
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateBytes("int", Array.emptyByteArray)

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateNString(1, "aaa")
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateNString("string", "bbb")

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateAsciiStream(
      1,
      new ByteArrayInputStream("Test me".getBytes())
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateAsciiStream(
      "string",
      new ByteArrayInputStream("Test me".getBytes())
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateAsciiStream(
      1,
      new ByteArrayInputStream("Test me".getBytes()),
      3
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateAsciiStream(
      "string",
      new ByteArrayInputStream("Test me".getBytes()),
      4
    )

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateBinaryStream(
      1,
      new ByteArrayInputStream("Test me".getBytes())
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateBinaryStream(
      "string",
      new ByteArrayInputStream("Test me".getBytes())
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateBinaryStream(
      1,
      new ByteArrayInputStream("Test me".getBytes()),
      3
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateBinaryStream(
      "string",
      new ByteArrayInputStream("Test me".getBytes()),
      4
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateBinaryStream(
      1,
      new ByteArrayInputStream("Test me".getBytes()),
      3L
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateBinaryStream(
      "string",
      new ByteArrayInputStream("Test me".getBytes()),
      4L
    )

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateBlob(
      1,
      new ByteArrayInputStream("Test me".getBytes())
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateBlob(
      "string",
      new ByteArrayInputStream("Test me".getBytes())
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateBlob(
      1,
      new ByteArrayInputStream("Test me".getBytes()),
      3L
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateBlob(
      "string",
      new ByteArrayInputStream("Test me".getBytes()),
      4L
    )

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateCharacterStream(
      1,
      new CharArrayReader("Test me".toCharArray)
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateCharacterStream(
      "string",
      new CharArrayReader("Test me".toCharArray)
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateCharacterStream(
      1,
      new CharArrayReader("Test me".toCharArray),
      3
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateCharacterStream(
      "string",
      new CharArrayReader("Test me".toCharArray),
      4
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateCharacterStream(
      1,
      new CharArrayReader("Test me".toCharArray),
      3L
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateCharacterStream(
      "string",
      new CharArrayReader("Test me".toCharArray),
      4L
    )

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateNCharacterStream(
      1,
      new CharArrayReader("Test me".toCharArray)
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateNCharacterStream(
      "string",
      new CharArrayReader("Test me".toCharArray)
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateNCharacterStream(
      1,
      new CharArrayReader("Test me".toCharArray),
      3L
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateNCharacterStream(
      "string",
      new CharArrayReader("Test me".toCharArray),
      4L
    )

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateClob(
      1,
      new CharArrayReader("Test me".toCharArray)
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateClob(
      "string",
      new CharArrayReader("Test me".toCharArray)
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateClob(
      1,
      new CharArrayReader("Test me".toCharArray),
      3L
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateClob(
      "string",
      new CharArrayReader("Test me".toCharArray),
      4L
    )

    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateNClob(
      1,
      new CharArrayReader("Test me".toCharArray)
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateNClob(
      "string",
      new CharArrayReader("Test me".toCharArray)
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateNClob(
      1,
      new CharArrayReader("Test me".toCharArray),
      3L
    )
    an[SQLFeatureNotSupportedException] should be thrownBy rs.updateNClob(
      "string",
      new CharArrayReader("Test me".toCharArray),
      4L
    )
  }

  private def createResultSet: YupanaResultSet = {
    val statement = mock[Statement]
    val result = SimpleResult(
      "test",
      Seq("int", "string", "double", "time"),
      Seq(DataType[Int], DataType[String], DataType[BigDecimal], DataType[Time]),
      Iterator(
        Array[Any](42, "foo", null, Time(1234567L))
      )
    )

    new YupanaResultSet(statement, result)
  }
}
