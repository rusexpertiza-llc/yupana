package org.yupana.jdbc

import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.proto.util.ProtocolVersion
import org.yupana.proto.{ Pong, Request, Response, Version }

import java.sql.{ ResultSet, SQLFeatureNotSupportedException }
import java.util.Properties
import scala.concurrent.Await
import scala.concurrent.duration._

class YupanaConnectionTest extends AnyFlatSpec with Matchers with OptionValues {

  "YupanaConnection" should "connect to server" in withConnection { connection =>
    connection.serverVersion.value shouldEqual Version(ProtocolVersion.value, 5, 25, "5.25.9")
    connection.isClosed shouldBe false
    connection.isValid(10) shouldBe true
    connection.close()
    connection.isClosed shouldBe true
    connection.isValid(10) shouldBe false
  }

  it should "throw exception on unsupported operations" in withConnection { c =>
    an[SQLFeatureNotSupportedException] should be thrownBy c.createNClob
    an[SQLFeatureNotSupportedException] should be thrownBy c.createClob
    an[SQLFeatureNotSupportedException] should be thrownBy c.createBlob
    an[SQLFeatureNotSupportedException] should be thrownBy c.createSQLXML
    an[SQLFeatureNotSupportedException] should be thrownBy c.createArrayOf("VARCHAR", Array("aaa", "bbb"))
    an[SQLFeatureNotSupportedException] should be thrownBy c.createStruct("Foo", Array())
    an[SQLFeatureNotSupportedException] should be thrownBy c.prepareCall(
      "SELECT x FROM y",
      ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY
    )
    an[SQLFeatureNotSupportedException] should be thrownBy c.prepareCall(
      "SELECT x FROM y",
      ResultSet.TYPE_FORWARD_ONLY,
      ResultSet.CONCUR_READ_ONLY,
      ResultSet.HOLD_CURSORS_OVER_COMMIT
    )
  }

  private def withConnection(test: YupanaConnectionImpl => Any): Unit = {
    val server = new ServerMock
    val props = new Properties()
    props.setProperty("yupana.host", "localhost")
    props.setProperty("yupana.port", server.port.toString)

    val reqF = server.handleRequestChunked { bs =>
      val ping = Request.parseFrom(bs).getPing
      val pong =
        Response(
          Response.Resp.Pong(
            Pong(ping.reqTime, System.currentTimeMillis(), Some(Version(ProtocolVersion.value, 5, 25, "5.25.9")))
          )
        )
      pong.toByteArray
    }

    val connection = new YupanaConnectionImpl(s"jdbc:yupana://localhost:${server.port}", props)
    Await.ready(reqF, 100.millis)

    test(connection)

    connection.close()
    server.close()
  }
}
