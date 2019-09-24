package org.yupana.jdbc

import java.io.IOException

import org.scalatest.{ FlatSpec, Matchers, OptionValues }
import org.yupana.jdbc.build.BuildInfo
import org.yupana.proto.util.ProtocolVersion
import org.yupana.proto.{ Ping, Pong, Request, Response, Version }

import scala.concurrent.Await
import scala.concurrent.duration._

class YupanaTcpClientTest extends FlatSpec with Matchers with OptionValues {
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
    the[IOException] thrownBy client.ping(12345) should have message "Broken pipe"
  }

}
