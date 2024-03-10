package org.yupana.jdbc

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.types.ByteReaderWriter
import org.yupana.protocol.{ Authorized, Credentials, CredentialsRequest, Hello, HelloResponse, ProtocolVersion }
import org.yupana.readerwriter.ByteBufferEvalReaderWriter

import java.nio.ByteBuffer
import java.sql.DriverManager
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class YupanaDriverTest extends AnyFlatSpec with Matchers {
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val rw: ByteReaderWriter[ByteBuffer] = ByteBufferEvalReaderWriter

  "YupanaDriver" should "connect using properly" in {
    Class.forName(classOf[YupanaDriver].getName)
    val server = new ServerMock

    val url = s"jdbc:yupana://127.0.0.1:${server.port}"

    val cf = for {
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
      c <- server
        .readAndSendResponses[Credentials](id, Credentials.readFrame[ByteBuffer], _ => Seq(Authorized()))
    } yield c

    val conn = DriverManager.getConnection(url, "test_user", "12345")

    val c = Await.result(cf, Duration.Inf)
    c.method shouldEqual CredentialsRequest.METHOD_PLAIN
    c.user shouldEqual Some("test_user")
    c.password shouldEqual Some("12345")

    conn.close()
  }

}
