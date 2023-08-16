package org.yupana.jdbc

import java.io.{ ByteArrayInputStream, CharArrayReader }
import java.net.URL
import java.sql.{ Date, SQLException, SQLFeatureNotSupportedException, Time, Timestamp, Types }
import java.util.Calendar
import org.scalamock.scalatest.MixedMockFactory
import org.yupana.api.query.SimpleResult
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.types.DataType
import org.yupana.jdbc.model.{ NumericValue, StringValue, TimestampValue }

class YupanaPreparedStatementTest extends AnyFlatSpec with Matchers with MixedMockFactory {

  "YupanaPreparedStatement" should "collect parameters" in {
    val conn = mock[YupanaConnection]
    val q = """SELECT item, kkmId FROM kkm_items
              |  WHERE time >= ? and time < ? and ItemsInvertedIndex_phrase = ?""".stripMargin

    val statement = new YupanaPreparedStatement(conn, q)

    statement.setTimestamp(1, new Timestamp(1578580000000L))
    statement.setString(3, "игрушка мягкая")
    statement.setTimestamp(2, new Timestamp(1578584211000L))

    (conn.runQuery _)
      .expects(
        q,
        Map(
          1 -> TimestampValue(1578580000000L),
          2 -> TimestampValue(1578584211000L),
          3 -> StringValue("игрушка мягкая")
        )
      )
      .returning(SimpleResult("dummy", Seq.empty, Seq.empty, Iterator.empty))

    statement.execute()
    statement.close()
  }

  it should "clear parameters" in {
    val conn = mock[YupanaConnection]
    val q = """SELECT item, kkmId FROM kkm_items
              |  WHERE time >= ? and time < ? and ItemsInvertedIndex_phrase = ?""".stripMargin

    val statement = new YupanaPreparedStatement(conn, q)

    statement.setTimestamp(1, new Timestamp(1578580000000L))
    statement.setString(3, "игрушка мягкая")

    statement.clearParameters()
    statement.setTimestamp(2, new Timestamp(1578584211000L))

    (conn.runQuery _)
      .expects(
        q,
        Map(
          2 -> TimestampValue(1578584211000L)
        )
      )
      .returning(SimpleResult("dummy", Seq.empty, Seq.empty, Iterator.empty))

    statement.execute()
  }

  it should "create batches" in {
    val conn = mock[YupanaConnection]
    val q = "UPSERT INTO (item, kkmId, time, sum, quantity) FROM kkm_items VALUES (?, ?, ?, ?, ?)"

    val statement = new YupanaPreparedStatement(conn, q)

    statement.setString(1, "молоко 1 пакет")
    statement.setString(2, "12345")
    statement.setTimestamp(3, new Timestamp(1578584211000L))
    statement.setFloat(4, 2.5f)
    statement.setLong(5, 2L)
    statement.addBatch()

    statement.setString(1, "колбаса докторская")
    statement.setString(2, "54321")
    statement.setDate(3, new Date(1578584212000L))
    statement.setDouble(4, 3.5d)
    statement.setShort(5, 3)
    statement.addBatch()

    (conn.runBatchQuery _)
      .expects(
        q,
        Seq(
          Map(
            1 -> StringValue("молоко 1 пакет"),
            2 -> StringValue("12345"),
            3 -> TimestampValue(1578584211000L),
            4 -> NumericValue(2.5f),
            5 -> NumericValue(2)
          ),
          Map(
            1 -> StringValue("колбаса докторская"),
            2 -> StringValue("54321"),
            3 -> TimestampValue(1578584212000L),
            4 -> NumericValue(3.5d),
            5 -> NumericValue(3)
          )
        )
      )
      .returning(SimpleResult("dummy", Seq.empty, Seq.empty, Iterator.empty))

    statement.executeBatch()
  }

  it should "support clear batch" in {
    val conn = mock[YupanaConnection]
    val q = "UPSERT INTO (item, kkmId, time, sum, quantity) FROM kkm_items VALUES (?, ?, ?, ?, ?)"

    val statement = new YupanaPreparedStatement(conn, q)

    statement.setString(1, "молоко 1 пакет")
    statement.setInt(2, 12345)
    statement.setTimestamp(3, new Timestamp(1578584211000L))
    statement.addBatch()

    statement.setString(1, "колбаса докторская")
    statement.setByte(2, 22)
    statement.setTimestamp(3, new Timestamp(1578584212000L))
    statement.setDouble(5, 1.5d)
    statement.setBigDecimal(4, new java.math.BigDecimal("1.40"))

    statement.clearBatch()
    statement.addBatch()

    (conn.runBatchQuery _)
      .expects(
        q,
        Seq(
          Map(
            1 -> StringValue("колбаса докторская"),
            2 -> NumericValue(22),
            3 -> TimestampValue(1578584212000L),
            4 -> NumericValue(1.40),
            5 -> NumericValue(1.5)
          )
        )
      )
      .returning(SimpleResult("dummy", Seq.empty, Seq.empty, Iterator.empty))

    statement.executeBatch()
  }

  it should "fail executeBatch if there are no batches" in {
    val conn = mock[YupanaConnection]
    val q = "UPSERT INTO (item, kkmId, time) FROM kkm_items VALUES (?, ?, ?)"

    val statement = new YupanaPreparedStatement(conn, q)
    an[SQLException] should be thrownBy statement.executeBatch()
  }

  it should "fail set unsupported types" in {
    val conn = mock[YupanaConnection]
    val q = "UPSERT INTO (item, kkmId, time) FROM kkm_items VALUES (?, ?, ?)"

    val statement = new YupanaPreparedStatement(conn, q)

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setURL(1, new URL("http", "localhost", "file"))

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setTimestamp(
      1,
      new Timestamp(12345L),
      Calendar.getInstance()
    )
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setTime(1, new Time(123456L))
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setTime(
      1,
      new Time(123456L),
      Calendar.getInstance()
    )

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setDate(
      1,
      new Date(123467L),
      Calendar.getInstance()
    )

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setObject(1, "test")
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setObject(1, "test", Types.VARCHAR)
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setObject(
      1,
      new java.math.BigDecimal(1234),
      Types.VARCHAR,
      2
    )

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setNull(1, Types.BIGINT)
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setNull(1, Types.BIGINT, "INTEGER")

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setAsciiStream(
      1,
      new ByteArrayInputStream("Test me".getBytes())
    )
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setAsciiStream(
      1,
      new ByteArrayInputStream("Test me".getBytes()),
      100
    )
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setAsciiStream(
      1,
      new ByteArrayInputStream("Test me".getBytes()),
      5L
    )

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setBinaryStream(
      1,
      new ByteArrayInputStream("Test me".getBytes())
    )
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setBinaryStream(
      1,
      new ByteArrayInputStream("Test me".getBytes()),
      100
    )
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setBinaryStream(
      1,
      new ByteArrayInputStream("Test me".getBytes()),
      5L
    )
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setUnicodeStream(
      1,
      new ByteArrayInputStream("Test me".getBytes()),
      10
    )

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setBlob(
      1,
      new ByteArrayInputStream("Test me".getBytes())
    )
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setBlob(
      1,
      new ByteArrayInputStream("Test me".getBytes()),
      100
    )

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setBlob(1, new YupanaBlob(Array.empty))

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setCharacterStream(
      1,
      new CharArrayReader("Test me".toCharArray)
    )
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setCharacterStream(
      2,
      new CharArrayReader("Test me".toCharArray),
      10L
    )
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setCharacterStream(
      1,
      new CharArrayReader("Test me".toCharArray),
      3
    )

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setNCharacterStream(
      1,
      new CharArrayReader("Test me".toCharArray)
    )
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setNCharacterStream(
      2,
      new CharArrayReader("Test me".toCharArray),
      10L
    )
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setNCharacterStream(
      1,
      new CharArrayReader("Test me".toCharArray),
      3
    )

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setNClob(
      1,
      new CharArrayReader("Test me".toCharArray)
    )
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setNClob(
      2,
      new CharArrayReader("Test me".toCharArray),
      10L
    )

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setClob(
      1,
      new CharArrayReader("Test me".toCharArray)
    )
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setClob(
      2,
      new CharArrayReader("Test me".toCharArray),
      10L
    )

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setArray(
      2,
      new YupanaArray[Int]("Test", Array(1, 2, 3), DataType[Int])
    )

    an[SQLFeatureNotSupportedException] should be thrownBy statement.setBoolean(1, x = true)
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setBytes(1, "Test me".getBytes())
    an[SQLFeatureNotSupportedException] should be thrownBy statement.setNString(2, "Test me")
  }

}
