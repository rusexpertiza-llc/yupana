package org.yupana.netty

import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http._
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.query.SimpleResult
import org.yupana.api.types.{ DataType, StringReaderWriter }
import org.yupana.core.QueryEngineRouter
import org.yupana.core.auth.{ NonEmptyUserAuthorizer, YupanaUser }
import org.yupana.core.sql.Parameter

import java.nio.charset.StandardCharsets

class HealthCheckTest extends AnyFlatSpec with Matchers with MockFactory {
  "HealthCheck" should "return ok on healthy TSDB" in {
    val queryEngine = mock[QueryEngineRouter]
    (queryEngine
      .query(_: YupanaUser, _: String, _: Map[Int, Parameter])(_: StringReaderWriter))
      .expects(YupanaUser.ANONYMOUS, "SELECT 1", Map.empty[Int, Parameter], *)
      .returning(
        Right(
          SimpleResult(
            "result",
            Seq("1"),
            Seq(DataType[BigDecimal]),
            Iterator(Array[Any](BigDecimal(1)))
          )
        )
      )

    val ch = new EmbeddedChannel(new HealthCheckHandler(ServerContext(queryEngine, NonEmptyUserAuthorizer)))

    ch.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/health-check"))
    val resp = ch.readOutbound[FullHttpResponse]()
    resp.status() shouldBe HttpResponseStatus.OK
    resp.content().toString(StandardCharsets.UTF_8) shouldEqual "Ok"
  }

  it should "return error on wrong path" in {
    val queryEngine = mock[QueryEngineRouter]
    val ch = new EmbeddedChannel(new HealthCheckHandler(ServerContext(queryEngine, NonEmptyUserAuthorizer)))

    ch.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/test"))
    val resp = ch.readOutbound[FullHttpResponse]()
    resp.status() shouldBe HttpResponseStatus.NOT_FOUND
  }

  it should "return error if query failed" in {
    val queryEngine = mock[QueryEngineRouter]
    (queryEngine
      .query(_: YupanaUser, _: String, _: Map[Int, Parameter])(_: StringReaderWriter))
      .expects(YupanaUser.ANONYMOUS, "SELECT 1", Map.empty[Int, Parameter], *)
      .returning(Left("I'm so tired"))

    val ch = new EmbeddedChannel(new HealthCheckHandler(ServerContext(queryEngine, NonEmptyUserAuthorizer)))

    ch.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_0, HttpMethod.GET, "/health-check"))
    val resp = ch.readOutbound[FullHttpResponse]()
    resp.status() shouldBe HttpResponseStatus.INTERNAL_SERVER_ERROR
    resp.content().toString(StandardCharsets.UTF_8) shouldBe "I'm so tired"
  }
}
