package org.yupana.jdbc

import java.sql.{ SQLException, Timestamp }

import org.scalamock.scalatest.MixedMockFactory
import org.scalatest.{ FlatSpec, Matchers }
import org.yupana.api.query.SimpleResult

class YupanaPreparedStatementTest extends FlatSpec with Matchers with MixedMockFactory {

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
    val q = "UPSERT INTO (item, kkmId, time) FROM kkm_items VALUES (?, ?, ?)"

    val statement = new YupanaPreparedStatement(conn, q)

    statement.setString(1, "молоко 1 пакет")
    statement.setString(2, "12345")
    statement.setTimestamp(3, new Timestamp(1578584211000L))
    statement.addBatch()

    statement.setString(1, "колбаса докторская")
    statement.setString(2, "54321")
    statement.setTimestamp(3, new Timestamp(1578584212000L))
    statement.addBatch()

    (conn.runBatchQuery _)
      .expects(
        q,
        Seq(
          Map(
            1 -> StringValue("молоко 1 пакет"),
            2 -> StringValue("12345"),
            3 -> TimestampValue(1578584211000L)
          ),
          Map(
            1 -> StringValue("колбаса докторская"),
            2 -> StringValue("54321"),
            3 -> TimestampValue(1578584212000L)
          )
        )
      )
      .returning(SimpleResult("dummy", Seq.empty, Seq.empty, Iterator.empty))

    statement.executeBatch()
  }

  it should "support clear batch" in {
    val conn = mock[YupanaConnection]
    val q = "UPSERT INTO (item, kkmId, time) FROM kkm_items VALUES (?, ?, ?)"

    val statement = new YupanaPreparedStatement(conn, q)

    statement.setString(1, "молоко 1 пакет")
    statement.setString(2, "12345")
    statement.setTimestamp(3, new Timestamp(1578584211000L))
    statement.addBatch()

    statement.setString(1, "колбаса докторская")
    statement.setString(2, "54321")
    statement.setTimestamp(3, new Timestamp(1578584212000L))

    statement.clearBatch()
    statement.addBatch()

    (conn.runBatchQuery _)
      .expects(
        q,
        Seq(
          Map(
            1 -> StringValue("колбаса докторская"),
            2 -> StringValue("54321"),
            3 -> TimestampValue(1578584212000L)
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

}
