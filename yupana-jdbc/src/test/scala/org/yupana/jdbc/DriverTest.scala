package org.yupana.jdbc

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.protocol.{ Authorized, Credentials, CredentialsRequest, Hello, HelloResponse, ProtocolVersion }

import java.nio.ByteBuffer
import java.sql.DriverManager

class DriverTest extends AnyFlatSpec with Matchers {
  import scala.concurrent.ExecutionContext.Implicits.global

  "YupanaDriver" should "connect using properly" in {
    Class.forName(classOf[YupanaDriver].getName)
    val server = new ServerMock

    val url = s"jdbc:yupana://127.0.0.1:${server.port}"

    for {
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
      r <- server
        .readAndSendResponses[Credentials](id, Credentials.readFrame[ByteBuffer], _ => Seq(Authorized()))
    } yield r

    val conn = DriverManager.getConnection(url, "test_user", "12345")

    conn.close()
  }

}
