package org.yupana.jdbc

import java.io.IOException
import org.scalatest.{ Inside, OptionValues }
import org.yupana.api.Time
import org.yupana.api.types.Storable

import scala.concurrent.Await
import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.jdbc.ServerMock.Step
import org.yupana.jdbc.build.BuildInfo
import org.yupana.protocol._

import java.nio.ByteBuffer

class YupanaTcpClientTest extends AnyFlatSpec with Matchers with OptionValues with Inside {
  import scala.concurrent.ExecutionContext.Implicits.global

  "TCP client" should "connect to the server" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")
    val reqF = server
      .read2AndSendResponses[Hello, Credentials](
        Step(Hello.readFrame[ByteBuffer])(h =>
          Seq(
            HelloResponse(ProtocolVersion.value, h.timestamp),
            CredentialsRequest(CredentialsRequest.METHOD_PLAIN)
          )
        ),
        Step(Credentials.readFrame[ByteBuffer])(_ => Seq(Authorized(), Idle()))
      )

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
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")
    server.readAndSendResponses(
      Step(Hello.readFrame[ByteBuffer])(_ => Seq(HelloResponse(ProtocolVersion.value + 1, 12345678)))
    )
    the[YupanaException] thrownBy client.connect(
      12345678
    ) should have message "Incompatible protocol versions: 4 on server and 3 in this driver"
  }

  it should "handle if response is too small" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")
    server.readAnySendRaw(Step(Hello.readFrame[ByteBuffer])(_ => Seq(Array(1.toByte))))
    the[IOException] thrownBy client.connect(12345) should have message "Unexpected end of response"
  }

  it should "handle if there are no response" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")
    server.closeOnReceive()
    an[IOException] should be thrownBy client.connect(12345)
  }

  it should "handle error response on hello" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")
    server.readAndSendResponses(Step(Hello.readFrame[ByteBuffer])(_ => Seq(ErrorMessage("Internal error"))))
    val e = the[YupanaException] thrownBy client.connect(23456789)
    e.getMessage should include("Internal error")
  }

  it should "fail on unexpected response on hello" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")
    val err = ResultHeader("table", Seq(ResultField("A", "VARCHAR")))
    server.readAndSendResponses(Step(Hello.readFrame[ByteBuffer])(_ => Seq(err)))
    the[YupanaException] thrownBy client.connect(
      23456789
    ) should have message "Unexpected response 'R' while waiting for 'H'"
  }

  it should "handle query" in withServerConnected { (server, client) =>

    val header = ResultHeader(
      "items_kkm",
      Seq(
        ResultField("time", "TIMESTAMP"),
        ResultField("item", "VARCHAR")
      )
    )

    val hb = Heartbeat(1)

    val ts = implicitly[Storable[Time]]
    val ss = implicitly[Storable[String]]

    val data1 = ResultRow(Seq(ts.write(Time(13333L)), ss.write("икра баклажанная")))

    val data2 = ResultRow(Seq(ts.write(Time(21112L)), Array.empty))

    val footer = ResultFooter(1, 2)

    val responses = Seq(header, hb, data1, data2, footer)

    val reqF = server.readAndSendResponses[PrepareQuery](Step(PrepareQuery.readFrame[ByteBuffer])(_ => responses))

    val sql =
      """
          |SELECT time, item FROM items_kkm
          |  WHERE time >= ? AND time < ? AND sum < ? AND item = ?
          |  """.stripMargin

    val result = client.prepareQuery(
      sql,
      Map(
        1 -> TimestampValue(12345L),
        2 -> TimestampValue(23456L),
        3 -> NumericValue(1000),
        4 -> StringValue("икра баклажанная")
      )
    )

    val req = Await.result(reqF, 100.millis)
    inside(req) {
      case PrepareQuery(q, f) =>
        q shouldEqual sql

    }

    result.name shouldEqual "items_kkm"

    val rows = result.toList

    rows(0).get[Time]("time") shouldEqual Time(13333L)
    rows(0).get[String]("item") shouldEqual "икра баклажанная"

    rows(1).get[Time]("time") shouldEqual Time(21112L)
    rows(1).get[String]("item") shouldEqual null

  }

  it should "handle batch query" in withServerConnected { (server, client) =>

    val header =
      ResultHeader(
        "items_kkm",
        Seq(
          ResultField("time", "TIMESTAMP"),
          ResultField("item", "VARCHAR")
        )
      )

    val hb = Heartbeat(1)

    val ts = implicitly[Storable[Time]]
    val ss = implicitly[Storable[String]]

    val data1 = ResultRow(Seq(ts.write(Time(13333L)), ss.write("икра баклажанная")))

    val data2 = ResultRow(Seq(ts.write(Time(21112L)), Array.empty))

    val footer = ResultFooter(1, 2)

    val responses = Seq(header, hb, data1, data2, footer)

    val reqF = server.readAndSendResponses(Step(PrepareQuery.readFrame[ByteBuffer])(_ => responses))

    val sql = """
                |SELECT time, item FROM items_kkm
                |  WHERE time >= ? AND time < ? AND sum < ? AND item = ?
                |  """.stripMargin

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

    val req = Await.result(reqF, 100.millis)
    inside(req) {
      case PrepareQuery(q, fs) =>
        q shouldEqual sql
        fs should have size 2
    }

    result.name shouldEqual "items_kkm"

    val rows = result.toList

    rows(0).get[Time]("time") shouldEqual Time(13333L)
    rows(0).get[String]("item") shouldEqual "икра баклажанная"

    rows(1).get[Time]("time") shouldEqual Time(21112L)
    rows(1).get[String]("item") shouldEqual null
  }

  it should "handle error response on query" in withServerConnected { (server, client) =>
    val err = ErrorMessage("Internal error")
    server.readAndSendResponses(Step(PrepareQuery.readFrame[ByteBuffer])(_ => Seq(err)))
    val e = the[IOException] thrownBy client.prepareQuery("SHOW TABLES", Map.empty)
    e.getMessage should include("Internal error")
  }

  it should "fail when no footer in result" in withServerConnected { (server, client) =>

    val header =
      ResultHeader(
        "items_kkm",
        Seq(
          ResultField("time", "TIMESTAMP"),
          ResultField("item", "VARCHAR")
        )
      )

    val hb = Heartbeat(1)

    val ts = implicitly[Storable[Time]]
    val ss = implicitly[Storable[String]]

    val data = ResultRow(Seq(ts.write(Time(13333L)), ss.write("икра баклажанная")))

    val responses = Seq(header, hb, data)

    val sql = """
                |SELECT time, item FROM items_kkm
                |  WHERE time >= ? AND time < ? AND sum < ? AND item = ?
                |  """.stripMargin

    server.readAndSendResponses(Step(PrepareQuery.readFrame[ByteBuffer])(_ => responses))

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

  private def withServerConnected(body: (ServerMock, YupanaTcpClient) => Unit): Unit = {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")
    val reqF = server
      .read2AndSendResponses[Hello, Credentials](
        Step(Hello.readFrame[ByteBuffer])(h =>
          Seq(
            HelloResponse(ProtocolVersion.value, h.timestamp),
            CredentialsRequest(CredentialsRequest.METHOD_PLAIN)
          )
        ),
        Step(Credentials.readFrame[ByteBuffer])(_ => Seq(Authorized(), Idle()))
      )

    client.connect(12345678L)
    Await.result(reqF, 100.millis)

    body(server, client)
    server.close()
  }
}
