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
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

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
              CredentialsRequest(CredentialsRequest.METHOD_PLAIN)
            )
        )
      credentials <- server
        .readAndSendResponses[Credentials](id, Credentials.readFrame[ByteBuffer], _ => Seq(Authorized(), Idle()))
      _ = server.close()
    } yield (hello, credentials)

    client.connect(12345678L)

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

    withServerConnected { (server, id) =>

      val responses = (q: PrepareQuery) => {
        val header = ResultHeader(
          q.id,
          "items_kkm",
          Seq(
            ResultField("time", "TIMESTAMP"),
            ResultField("item", "VARCHAR")
          )
        )

        val hb = Heartbeat(1)

        val ts = implicitly[Storable[Time]]
        val ss = implicitly[Storable[String]]

        val data1 = ResultRow(q.id, Seq(ts.write(Time(13333L)), ss.write("икра баклажанная")))

        val data2 = ResultRow(q.id, Seq(ts.write(Time(21112L)), Array.empty))

        val footer = ResultFooter(q.id, 1, 2)

        Seq(header, hb, data1, data2, footer, Idle())
      }

      val reqF = server.readAndSendResponses[PrepareQuery](id, PrepareQuery.readFrame[ByteBuffer], responses)

      reqF.map { req =>
        inside(req) {
          case PrepareQuery(_, q, f) =>
            q shouldEqual sql

        }
      }
    } { client =>
      val result = client.prepareQuery(
        sql,
        Map(
          1 -> TimestampValue(12345L),
          2 -> TimestampValue(23456L),
          3 -> NumericValue(1000),
          4 -> StringValue("икра баклажанная")
        )
      )

      result.name shouldEqual "items_kkm"

      val rows = result.toList

      rows(0).get[Time]("time") shouldEqual Time(13333L)
      rows(0).get[String]("item") shouldEqual "икра баклажанная"

      rows(1).get[Time]("time") shouldEqual Time(21112L)
      rows(1).get[String]("item") shouldEqual null
    }
  }

  ignore should "handle batch query" in {
    val sql =
      """
        |SELECT time, item FROM items_kkm
        |  WHERE time >= ? AND time < ? AND sum < ? AND item = ?
        |  """.stripMargin

    withServerConnected { (server, id) =>

      val responses = (q: PrepareQuery) => {
        val header =
          ResultHeader(
            q.id,
            "items_kkm",
            Seq(
              ResultField("time", "TIMESTAMP"),
              ResultField("item", "VARCHAR")
            )
          )

        val hb = Heartbeat(1)

        val ts = implicitly[Storable[Time]]
        val ss = implicitly[Storable[String]]

        val data1 = ResultRow(q.id, Seq(ts.write(Time(13333L)), ss.write("икра баклажанная")))

        val data2 = ResultRow(q.id, Seq(ts.write(Time(21112L)), Array.empty))

        val footer = ResultFooter(q.id, 1, 2)

        Seq(header, hb, data1, data2, footer)
      }

      val reqF = server.readAndSendResponses[PrepareQuery](id, PrepareQuery.readFrame[ByteBuffer], responses)
      reqF map { req =>
        inside(req) {
          case PrepareQuery(_, q, fs) =>
            q shouldEqual sql
            fs should have size 2
        }
      }
    } { client =>
      val result = client.batchQuery(
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

      result.name shouldEqual "items_kkm"

      val rows = result.toList

      rows(0).get[Time]("time") shouldEqual Time(13333L)
      rows(0).get[String]("item") shouldEqual "икра баклажанная"

      rows(1).get[Time]("time") shouldEqual Time(21112L)
      rows(1).get[String]("item") shouldEqual null
    }
  }

  it should "handle error response on query" in withServerConnected { (server, id) =>
    val err = ErrorMessage("Internal error")
    server.readAndSendResponses[PrepareQuery](id, PrepareQuery.readFrame[ByteBuffer], _ => Seq(err))
  } { client =>
    val e = the[YupanaException] thrownBy client.prepareQuery("SHOW TABLES", Map.empty)
    e.getMessage should include("Internal error")
  }

  it should "fail when no footer in result" in withServerConnected { (server, id) =>

    val responses = (q: PrepareQuery) => {
      val header =
        ResultHeader(
          q.id,
          "items_kkm",
          Seq(
            ResultField("time", "TIMESTAMP"),
            ResultField("item", "VARCHAR")
          )
        )

      val hb = Heartbeat(1)

      val ts = implicitly[Storable[Time]]
      val ss = implicitly[Storable[String]]

      val data = ResultRow(q.id, Seq(ts.write(Time(13333L)), ss.write("икра баклажанная")))

      Seq(header, hb, data)
    }

    server.readAndSendResponses[PrepareQuery](id, PrepareQuery.readFrame[ByteBuffer], responses)
  } { client =>

    val sql =
      """
        |SELECT time, item FROM items_kkm
        |  WHERE time >= ? AND time < ? AND sum < ? AND item = ?
        |  """.stripMargin

    the[IOException] thrownBy {
      val res = client.prepareQuery(
        sql,
        Map(
          1 -> TimestampValue(12345L),
          2 -> TimestampValue(23456L),
          3 -> NumericValue(1000),
          4 -> StringValue("икра баклажанная")
        )
      )
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
            Seq(HelloResponse(ProtocolVersion.value, h.timestamp), CredentialsRequest(CredentialsRequest.METHOD_PLAIN))
        )
      _ <- server
        .readAndSendResponses[Credentials](id, Credentials.readFrame[ByteBuffer], _ => Seq(Authorized(), Idle()))
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
