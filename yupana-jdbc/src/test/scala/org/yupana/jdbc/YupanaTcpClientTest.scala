package org.yupana.jdbc

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{ Inside, OptionValues }
import org.yupana.api.Time
import org.yupana.api.types.{ ByteReaderWriter, ID, Storable }
import org.yupana.jdbc.build.BuildInfo
import org.yupana.protocol._
import org.yupana.serialization.ByteBufferEvalReaderWriter

import java.io.IOException
import java.nio.ByteBuffer
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

class YupanaTcpClientTest extends AnyFlatSpec with Matchers with OptionValues with Inside {
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val rw: ByteReaderWriter[ByteBuffer] = ByteBufferEvalReaderWriter

  "TCP client" should "connect to the server" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, 100, Some("user"), Some("password"))
    val reqF = for {
      id <- server.connect
      hello <- server
        .readAndSendResponses[Hello](
          id,
          Hello.readFrame[ByteBuffer],
          h =>
            Seq(
              HelloResponse(ProtocolVersion.value, h.timestamp),
              CredentialsRequest(Seq(CredentialsRequest.METHOD_PLAIN, "SECURE"))
            )
        )
      credentials <- server
        .readAndSendResponses[Credentials](id, Credentials.readFrame[ByteBuffer], _ => Seq(Authorized()))
      _ <- server.readAndSendResponses[Quit](id, Quit.readFrame[ByteBuffer], _ => Nil)
      _ = server.close()
    } yield (hello, credentials)

    client.connect(12345678L)
    client.close()

    val (hello, credentials) = Await.result(reqF, 100.millis)
    hello shouldEqual Hello(
      ProtocolVersion.value,
      BuildInfo.version,
      12345678L,
      Map.empty
    )

    credentials shouldEqual Credentials(CredentialsRequest.METHOD_PLAIN, Some("user"), Some("password"))

    server.close()
  }

  it should "fail if protocol version does not match" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, 10, Some("user"), Some("password"))
    server.connect.flatMap(id =>
      server.readAndSendResponses[Hello](
        id,
        Hello.readFrame[ByteBuffer],
        _ => Seq(HelloResponse(ProtocolVersion.value + 1, 12345678))
      )
    )
    the[YupanaException] thrownBy client.connect(
      12345678
    ) should have message "Incompatible protocol versions: 4 on server and 3 in this driver"
  }

  it should "handle if response is too small" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, 100, Some("user"), Some("password"))
    for {
      id <- server.connect
      _ <- server.readAnySendRaw[Hello](id, Hello.readFrame[ByteBuffer], _ => Seq(Array(1.toByte)))
      _ = server.close(id)
    } yield ()
    the[IOException] thrownBy client.connect(12345) should have message "Unexpected end of response"
  }

  it should "handle if there are no response" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, 5, Some("user"), Some("password"))
    server.connect.foreach(server.close)
    an[IOException] should be thrownBy client.connect(12345)
  }

  it should "handle error response on hello" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, 10, Some("user"), Some("password"))
    server.connect.flatMap(id =>
      server.readAndSendResponses[Hello](id, Hello.readFrame[ByteBuffer], _ => Seq(ErrorMessage("Internal error")))
    )
    val e = the[YupanaException] thrownBy client.connect(23456789)
    e.getMessage should include("Internal error")
  }

  it should "fail on unexpected response on hello" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, 10, Some("user"), Some("password"))
    val err = ResultHeader(1, "table", Seq(ResultField("A", "VARCHAR")))
    server.connect.flatMap(id => server.readAndSendResponses[Hello](id, Hello.readFrame[ByteBuffer], _ => Seq(err)))
    the[YupanaException] thrownBy client.connect(
      23456789
    ) should have message "Unexpected response 'R' while waiting for 'H'"
  }

  it should "handle query" in {
    val sql =
      """
        |SELECT time, item FROM items_kkm
        |  WHERE time >= ? AND time < ? AND sum < ? AND item = ?
        |  """.stripMargin

    val bind = Map(
      1 -> TimestampValue(12345L),
      2 -> TimestampValue(23456L),
      3 -> NumericValue(1000),
      4 -> StringValue("икра баклажанная")
    )

    withServerConnected { (server, id) =>

      val onQuery = (q: SqlQuery) => {
        val header = ResultHeader(
          q.id,
          "items_kkm",
          Seq(
            ResultField("time", "TIMESTAMP"),
            ResultField("item", "VARCHAR")
          )
        )

        Seq(header)
      }

      val onNext = (n: NextBatch) => {

        val data1 = ResultRow(n.id, Seq(toBytes(Time(13333L)), toBytes("икра баклажанная")))

        val data2 = ResultRow(n.id, Seq(toBytes(Time(21112L)), Array.empty))

        val footer = ResultFooter(n.id, 1, 2)

        Seq(data1, data2, footer)
      }

      val reqF = for {
        req <- server.readAndSendResponses[SqlQuery](id, SqlQuery.readFrame[ByteBuffer], onQuery)
        next <- server.readAndSendResponses[NextBatch](id, NextBatch.readFrame[ByteBuffer], onNext)
      } yield (req, next)

      reqF.map { req =>
        inside(req) {
          case (SqlQuery(qId, q, f), NextBatch(nId, bs)) =>
            q shouldEqual sql
            f shouldEqual bind
            qId shouldEqual nId
            bs shouldEqual 10
        }
      }
    } { client =>
      val result = client.prepareQuery(sql, bind).result

      result.name shouldEqual "items_kkm"

      result.next() shouldEqual true

      result.get[Time]("time") shouldEqual Time(13333L)
      result.get[String]("item") shouldEqual "икра баклажанная"

      result.next() shouldEqual true

      result.get[Time]("time") shouldEqual Time(21112L)
      result.get[String]("item") shouldEqual null
    }
  }

  it should "read batch correctly" in {
    val sql = "SELECT x FROM table"

    withServerConnected { (server, id) =>
      val onQuery = (q: SqlQuery) => Seq(ResultHeader(q.id, "table", Seq(ResultField("x", "INTEGER"))))
      val onNext1 = (n: NextBatch) => 1 to 10 map (x => ResultRow(n.id, Seq(toBytes(x))))
      val onNext2 = (n: NextBatch) => {
        val rows = 11 to 15 map (x => ResultRow(n.id, Seq(toBytes(x))))
        val footer = ResultFooter(n.id, -1, 15)
        rows :+ footer
      }

      for {
        r <- server.readAndSendResponses(id, SqlQuery.readFrame[ByteBuffer], onQuery)
        n1 <- server.readAndSendResponses(id, NextBatch.readFrame[ByteBuffer], onNext1)
        n2 <- server.readAndSendResponses(id, NextBatch.readFrame[ByteBuffer], onNext2)
      } yield (r, n1, n2)
    } { client =>
      val result = client.prepareQuery(sql, Map.empty).result
      result.name shouldEqual "table"

      var r = Seq.empty[Int]
      while (result.next()) {
        r = r :+ result.get[Int](0)
      }
      r should contain theSameElementsInOrderAs (1 to 15)
    }
  }

  it should "handle batch query" in {
    val sql =
      """
        |SELECT time, item FROM items_kkm
        |  WHERE time >= ? AND time < ? AND sum < ? AND item = ?
        |  """.stripMargin

    withServerConnected { (server, id) =>

      val onQuery = (q: BatchQuery) => {
        Seq(
          ResultHeader(
            q.id,
            "items_kkm",
            Seq(
              ResultField("time", "TIMESTAMP"),
              ResultField("item", "VARCHAR")
            )
          )
        )
      }

      val onNext = (n: NextBatch) => {

        val data1 = ResultRow(n.id, Seq(toBytes(Time(13333L)), toBytes("икра баклажанная")))

        val data2 = ResultRow(n.id, Seq(toBytes(Time(21112L)), Array.empty))

        val footer = ResultFooter(n.id, 1, 2)

        Seq(data1, data2, footer)
      }

      val reqF = for {
        req <- server.readAndSendResponses[BatchQuery](id, BatchQuery.readFrame[ByteBuffer], onQuery)
        _ <- server.readAndSendResponses[NextBatch](id, NextBatch.readFrame[ByteBuffer], onNext)
      } yield req

      reqF map { req =>
        inside(req) {
          case BatchQuery(_, q, fs) =>
            q shouldEqual sql
            fs should have size 2
        }
      }
    } { client =>
      val result = client
        .batchQuery(
          sql,
          Seq(
            Map(
              1 -> TimestampValue(12345L),
              2 -> TimestampValue(23456L),
              3 -> NumericValue(1000),
              4 -> StringValue("икра баклажанная")
            ),
            Map(
              1 -> TimestampValue(12345L),
              2 -> TimestampValue(23456L),
              3 -> NumericValue(100),
              4 -> StringValue("икра кабачковая")
            )
          )
        )
        .result

      result.name shouldEqual "items_kkm"

      result.next() shouldEqual true
      result.get[Time]("time") shouldEqual Time(13333L)
      result.get[String]("item") shouldEqual "икра баклажанная"

      result.next() shouldEqual true
      result.get[Time]("time") shouldEqual Time(21112L)
      result.get[String]("item") shouldEqual null
    }
  }

  it should "handle global error response on query" in withServerConnected { (server, id) =>
    server.readAndSendResponses[SqlQuery](id, SqlQuery.readFrame[ByteBuffer], _ => Seq(ErrorMessage("Internal error")))
  } { client =>
    val e = the[YupanaException] thrownBy client.prepareQuery("SHOW TABLES", Map.empty)
    e.getMessage should include("Internal error")
  }

  it should "handle stream error response on query" in withServerConnected { (server, id) =>
    server.readAndSendResponses[SqlQuery](
      id,
      SqlQuery.readFrame[ByteBuffer],
      q => Seq(ErrorMessage("Internal error", Some(q.id)))
    )
  } { client =>
    val e = the[YupanaException] thrownBy client.prepareQuery("SHOW TABLES", Map.empty)
    e.getMessage should include("Internal error")
  }

  it should "fail when no footer in result" in withServerConnected { (server, id) =>

    val onQuery = (q: SqlQuery) => {
      val header =
        ResultHeader(
          q.id,
          "items_kkm",
          Seq(
            ResultField("time", "TIMESTAMP"),
            ResultField("item", "VARCHAR")
          )
        )
      Seq(header)
    }

    val onNext = (n: NextBatch) => {
      Seq(ResultRow(n.id, Seq(toBytes(Time(13333L)), toBytes("икра баклажанная"))))
    }

    for {
      _ <- server.readAndSendResponses[SqlQuery](id, SqlQuery.readFrame[ByteBuffer], onQuery)
      _ <- server.readAndSendResponses[NextBatch](id, NextBatch.readFrame[ByteBuffer], onNext)
    } yield ()

  } { client =>

    val sql =
      """
        |SELECT time, item FROM items_kkm
        |  WHERE time >= ? AND time < ? AND sum < ? AND item = ?
        |  """.stripMargin

    the[IOException] thrownBy {
      val res = client
        .prepareQuery(
          sql,
          Map(
            1 -> TimestampValue(12345L),
            2 -> TimestampValue(23456L),
            3 -> NumericValue(1000),
            4 -> StringValue("икра баклажанная")
          )
        )
        .result
      res.next()
    } should have message "Unexpected end of response"
  }

  it should "operate normally after streaming error" in withServerConnected { (server, id) =>
    val onQuery1 = (q: SqlQuery) => {
      println(s"Q1 = $q")
      Seq(ErrorMessage("Invalid statement SELEKT", Some(q.id)))
    }
    val onQuery2 = (q: SqlQuery) => {
      println(s"Q2 = $q")
      Seq(ResultHeader(q.id, "result", Seq(ResultField("1", "INTEGER"))))
    }

    for {
      e <- server.readAndSendResponses(id, SqlQuery.readFrame[ByteBuffer], onQuery1)
      r <- server.readAndSendResponses(id, SqlQuery.readFrame[ByteBuffer], onQuery2)
    } yield Seq(e, r)

  } { client =>
    val e = the[YupanaException] thrownBy client.prepareQuery("SELEKT 1", Map.empty)
    e.getMessage should include("Invalid statement SELEKT")

    val r = client.prepareQuery("SELECT 1", Map.empty).result
    r.name shouldBe "result"
  }

  private def withServerConnected[T](
      serverBody: (ServerMock, Int) => Future[T]
  )(clientBody: YupanaTcpClient => Unit): T = {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, 10, Some("user"), Some("password"))
    val f = for {
      id <- server.connect
      _ <- server
        .readAndSendResponses[Hello](
          id,
          Hello.readFrame[ByteBuffer],
          h =>
            Seq(
              HelloResponse(ProtocolVersion.value, h.timestamp),
              CredentialsRequest(Seq(CredentialsRequest.METHOD_PLAIN))
            )
        )
      _ <- server
        .readAndSendResponses[Credentials](id, Credentials.readFrame[ByteBuffer], _ => Seq(Authorized()))
      r <- serverBody(server, id)
      _ = server.close(id)
    } yield r
    client.connect(12345678L)
    clientBody(client)
    val result = Await.result(f, 100.millis)
    client.close()

    server.close()
    result
  }

  private def toBytes[T](v: T)(implicit st: Storable[T]): Array[Byte] = {
    val b = ByteBuffer.allocate(1024)
    implicit val rw: ByteReaderWriter[ByteBuffer] = ByteBufferEvalReaderWriter
    st.write(b, v: ID[T])
    val size = b.position()
    b.rewind()
    val res = Array.ofDim[Byte](size)
    b.get(res)
    res
  }
}
