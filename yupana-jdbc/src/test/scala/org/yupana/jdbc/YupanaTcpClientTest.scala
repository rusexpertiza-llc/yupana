package org.yupana.jdbc

import java.io.IOException
import org.scalatest.{ Inside, OptionValues }
import org.yupana.api.Time
import org.yupana.api.types.Storable

import scala.concurrent.Await
import scala.concurrent.duration._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.jdbc.build.BuildInfo
import org.yupana.protocol._

import java.nio.ByteBuffer

class YupanaTcpClientTest extends AnyFlatSpec with Matchers with OptionValues with Inside {
  import scala.concurrent.ExecutionContext.Implicits.global

  "TCP client" should "connect to the server" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")
    val pong = HelloResponse(ProtocolVersion.value, 12345678L)
    val authReq = CredentialsRequest(CredentialsRequest.METHOD_PLAIN)
    val reqF = server
      .readBytesSendResponses(Seq(pong, authReq))
      .map(Hello.readFrame[ByteBuffer])
    client.connect(12345678L)

    val req = Await.result(reqF, 100.millis)
    req shouldEqual Hello(
      ProtocolVersion.value,
      BuildInfo.version,
      12345678L,
      Map.empty
    )

    val req2F = server.readBytesSendResponses(Seq(Authorized(), Idle())).map(Credentials.readFrame[ByteBuffer])

    val req2 = Await.result(req2F, 100.millis)
    req2 shouldEqual Credentials(CredentialsRequest.METHOD_PLAIN, "user", "password")

    server.close()
  }

  it should "fail if protocol version does not match" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")
    val pong = HelloResponse(ProtocolVersion.value + 1, 12345678)
    server.readBytesSendResponse(pong)
    the[IOException] thrownBy client.connect(
      12345678
    ) should have message "Incompatible protocol versions: 4 on server and 3 in this driver"
  }

  it should "handle if response is too small" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")
    server.readBytesSendResponse(Array(1.toByte))
    the[IOException] thrownBy client.connect(12345) should have message "Unexpected end of response"
  }

  it should "handle if there are no response" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")
    server.closeOnReceive()
    an[IOException] should be thrownBy client.connect(12345)
  }

  it should "handle error response on ping" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")
    val err = ErrorMessage("Internal error")
    server.readBytesSendResponse(err)
    val e = the[IOException] thrownBy client.connect(23456789)
    e.getMessage should include("Internal error")
  }

  it should "fail on unexpected response on ping" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")
    val err = ResultHeader("table", Seq(ResultField("A", "VARCHAR")))
    server.readBytesSendResponse(err)
    the[IOException] thrownBy client.connect(23456789) should have message "Unexpected response 'R' on ping"
  }

  it should "handle query" in {

    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")

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

    val reqF = server.readBytesSendResponses(responses).map(PrepareQuery.readFrame[ByteBuffer])

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

  it should "handle batch query" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")

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

    val reqF = server.readBytesSendResponses(responses).map(PrepareQuery.readFrame[ByteBuffer])

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

  it should "handle error response on query" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")
    val err = ErrorMessage("Internal error")
    server.readBytesSendResponse(err)
    val e = the[IllegalArgumentException] thrownBy client.prepareQuery("SHOW TABLES", Map.empty)
    e.getMessage should include("Internal error")
  }

  it should "fail when no footer in result" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port, "user", "password")

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

    server.readBytesSendResponses(responses).map(PrepareQuery.readFrame[ByteBuffer])

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
}
