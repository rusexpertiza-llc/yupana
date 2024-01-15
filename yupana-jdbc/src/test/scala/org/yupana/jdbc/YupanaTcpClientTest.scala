package org.yupana.jdbc

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{ Inside, OptionValues }
import org.yupana.api.Time
import org.yupana.api.types.Storable
import org.yupana.jdbc.build.BuildInfo
import org.yupana.protocol._

import java.io.IOException
import java.nio.ByteBuffer
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

class YupanaTcpClientTest extends AnyFlatSpec with Matchers with OptionValues with Inside {
  import scala.concurrent.ExecutionContext.Implicits.global

  "TCP client" should "connect to the server" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, 100, "user", "password")
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

    credentials shouldEqual Credentials(CredentialsRequest.METHOD_PLAIN, "user", "password")

    server.close()
  }

  it should "fail if protocol version does not match" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, 10, "user", "password")
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
    val client = new YupanaTcpClient("127.0.0.1", server.port, 100, "user", "password")
    for {
      id <- server.connect
      _ <- server.readAnySendRaw[Hello](id, Hello.readFrame[ByteBuffer], _ => Seq(Array(1.toByte)))
      _ = server.close(id)
    } yield ()
    the[IOException] thrownBy client.connect(12345) should have message "Unexpected end of response"
  }

  it should "handle if there are no response" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, 5, "user", "password")
    server.connect.foreach(server.close)
    an[IOException] should be thrownBy client.connect(12345)
  }

  it should "handle error response on hello" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, 10, "user", "password")
    server.connect.flatMap(id =>
      server.readAndSendResponses[Hello](id, Hello.readFrame[ByteBuffer], _ => Seq(ErrorMessage("Internal error")))
    )
    val e = the[YupanaException] thrownBy client.connect(23456789)
    e.getMessage should include("Internal error")
  }

  it should "fail on unexpected response on hello" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, 10, "user", "password")
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
        val ts = implicitly[Storable[Time]]
        val ss = implicitly[Storable[String]]

        val data1 = ResultRow(n.id, Seq(ts.write(Time(13333L)), ss.write("икра баклажанная")))

        val data2 = ResultRow(n.id, Seq(ts.write(Time(21112L)), Array.empty))

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

      val rows = result.toList

      rows(0).get[Time]("time") shouldEqual Time(13333L)
      rows(0).get[String]("item") shouldEqual "икра баклажанная"

      rows(1).get[Time]("time") shouldEqual Time(21112L)
      rows(1).get[String]("item") shouldEqual null
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
        val ts = implicitly[Storable[Time]]
        val ss = implicitly[Storable[String]]

        val data1 = ResultRow(n.id, Seq(ts.write(Time(13333L)), ss.write("икра баклажанная")))

        val data2 = ResultRow(n.id, Seq(ts.write(Time(21112L)), Array.empty))

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

      val rows = result.toList

      rows(0).get[Time]("time") shouldEqual Time(13333L)
      rows(0).get[String]("item") shouldEqual "икра баклажанная"

      rows(1).get[Time]("time") shouldEqual Time(21112L)
      rows(1).get[String]("item") shouldEqual null
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
      val ts = implicitly[Storable[Time]]
      val ss = implicitly[Storable[String]]
      Seq(ResultRow(n.id, Seq(ts.write(Time(13333L)), ss.write("икра баклажанная"))))
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

  private def withServerConnected[T](
      serverBody: (ServerMock, Int) => Future[T]
  )(clientBody: YupanaTcpClient => Unit): T = {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, 10, "user", "password")
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

    server.close()
    result
  }
}
