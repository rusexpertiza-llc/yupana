package org.yupana.jdbc

import java.io.IOException

import com.google.protobuf.ByteString
import org.scalatest.{ FlatSpec, Inside, Matchers, OptionValues }
import org.yupana.api.Time
import org.yupana.api.types.Writable
import org.yupana.jdbc.build.BuildInfo
import org.yupana.proto.util.ProtocolVersion
import org.yupana.proto._

import scala.concurrent.Await
import scala.concurrent.duration._

class YupanaTcpClientTest extends FlatSpec with Matchers with OptionValues with Inside {
  import scala.concurrent.ExecutionContext.Implicits.global

  "TCP client" should "handle ping/pong" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port)
    val pong =
      Response(Response.Resp.Pong(Pong(12345678, 12345679, Some(Version(ProtocolVersion.value, 5, 4, "5.4.22")))))
    val reqF = server.readBytesSendResponseChunked(pong.toByteArray).map(Request.parseFrom)
    val version = client.ping(12345678)

    version.value shouldEqual Version(ProtocolVersion.value, 5, 4, "5.4.22")
    val req = Await.result(reqF, 100.millis)
    req.getPing shouldEqual Ping(
      12345678,
      Some(Version(ProtocolVersion.value, BuildInfo.majorVersion, BuildInfo.minorVersion, BuildInfo.version))
    )

    server.close()
  }

  it should "fail if protocol version does not match" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port)
    val pong =
      Response(Response.Resp.Pong(Pong(12345678, 12345679, Some(Version(ProtocolVersion.value + 1, 5, 4, "5.4.22")))))
    server.readBytesSendResponseChunked(pong.toByteArray)
    the[IOException] thrownBy client.ping(12345678) should have message "Incompatible protocol versions: 3 on server and 2 in this driver"
  }

  it should "handle if response is too small" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port)
    server.readBytesSendResponse(Array(1))
    the[IOException] thrownBy client.ping(12345) should have message "Invalid server response"
  }

  it should "handle if there are no response" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port)
    server.closeOnReceive()
    an[IOException] should be thrownBy client.ping(12345)
  }

  it should "handle query" in {
    val server = new ServerMock
    val client = new YupanaTcpClient("127.0.0.1", server.port)

    val header = Response(
      Response.Resp.ResultHeader(
        ResultHeader(
          Seq(
            ResultField("time", "TIMESTAMP"),
            ResultField("item", "VARCHAR")
          ),
          Some("items_kkm")
        )
      )
    )

    val hb = Response(
      Response.Resp.Heartbeat("1")
    )

    val tw = implicitly[Writable[Time]]
    val sw = implicitly[Writable[String]]

    val data1 = Response(
      Response.Resp.Result(
        ResultChunk(Seq(ByteString.copyFrom(tw.write(Time(13333L))), ByteString.copyFrom(sw.write("икра баклажанная"))))
      )
    )

    val data2 = Response(
      Response.Resp.Result(
        ResultChunk(Seq(ByteString.copyFrom(tw.write(Time(21112L))), ByteString.copyFrom(sw.write("икра баклажанная"))))
      )
    )

    val footer = Response(
      Response.Resp.ResultStatistics(
        ResultStatistics(1, 2)
      )
    )

    val responses = Seq(header, hb, data1, data2, footer).map(_.toByteArray)

    val reqF = server.readBytesSendResponsesChunked(responses).map(Request.parseFrom)

    val sql = """
                |SELECT time, item FROM items_kkm 
                |  WHERE time >= ? AND time < ? AND sum < ? AND item = ?
                |  """.stripMargin

    val result = client.query(
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
      case Request(Request.Req.SqlQuery(SqlQuery(q, f))) =>
        q shouldEqual sql

    }

    result.name shouldEqual "items_kkm"

    val rows = result.toIterator.toList

    rows(0).fieldValueByName[Time]("time").value shouldEqual Time(13333L)
    rows(0).fieldValueByName[String]("item").value shouldEqual "икра баклажанная"

    rows(1).fieldValueByName[Time]("time").value shouldEqual Time(21112L)
    rows(1).fieldValueByName[String]("item").value shouldEqual "икра баклажанная"
  }

}
