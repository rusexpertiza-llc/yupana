package org.yupana.netty

import io.netty.buffer.ByteBuf
import io.netty.channel.embedded.EmbeddedChannel
import org.scalamock.scalatest.MockFactory
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.SimpleResult
import org.yupana.api.types.DataType
import org.yupana.core.QueryEngineRouter
import org.yupana.protocol._

class MessageHandlerTest extends AnyFlatSpec with Matchers with GivenWhenThen with MockFactory {

  import NettyBuffer._

  "MessageHandler" should "establish connection" in {
    Given("Message Handler")
    val ch = new EmbeddedChannel(new MessageHandler(ServerContext(null, new NonEmptyUserAuthorizer, None)))

    val cmd = new Hello(ProtocolVersion.value, "3.2.1", 1234567L, Map.empty)
    val frame = cmd.toFrame[ByteBuf]

    When("Hello command received")
    ch.writeInbound(frame) //  shouldBe true

    val respFrame = ch.readOutbound[Frame]()
    Then("Hello response and credentials req should be replied")
    respFrame.frameType shouldEqual Tags.HELLO_RESPONSE
    val resp = HelloResponse.readFrame(respFrame)

    resp.protocolVersion shouldBe ProtocolVersion.value
    resp.reqTime shouldEqual 1234567L

    val credentialsFrame = ch.readOutbound[Frame]()
    credentialsFrame.frameType shouldEqual Tags.CREDENTIALS_REQUEST
    val credentials = CredentialsRequest.readFrame(credentialsFrame)
    credentials.method shouldEqual CredentialsRequest.METHOD_PLAIN

    When("Credentials sent")

    ch.writeInbound(Credentials(CredentialsRequest.METHOD_PLAIN, "test", "pass").toFrame[ByteBuf])
    ch.finish() shouldBe true

    Then("Authorized should be replied")
    val authFrame = ch.readOutbound[Frame]()
    authFrame.frameType shouldEqual Tags.AUTHORIZED

    Then("Idle shall be replied")
    val idleFrame = ch.readOutbound[Frame]()
    idleFrame.frameType shouldEqual Tags.IDLE
  }

  it should "fail on unknown auth method" in {
    val ch = new EmbeddedChannel(new MessageHandler(ServerContext(null, new NonEmptyUserAuthorizer, None)))

    val cmd = new Hello(ProtocolVersion.value, "3.2.1", 1234567L, Map.empty)
    val frame = cmd.toFrame[ByteBuf]

    ch.writeInbound(frame)
    val respFrame = ch.readOutbound[Frame]()
    respFrame.frameType shouldEqual Tags.HELLO_RESPONSE
    val credentialsFrame = ch.readOutbound[Frame]()
    credentialsFrame.frameType shouldEqual Tags.CREDENTIALS_REQUEST

    ch.writeInbound(Credentials("SECURE", "test", "pass").toFrame[ByteBuf])
    ch.finish() shouldBe true

    val errFrame = ch.readOutbound[Frame]()
    ch.isOpen shouldBe false
    errFrame.frameType shouldEqual Tags.ERROR_MESSAGE
    val err = ErrorMessage.readFrame(errFrame)
    err.message shouldEqual "Unsupported auth method 'SECURE'"
    err.severity shouldEqual ErrorMessage.SEVERITY_FATAL
  }

  it should "fail with empty user" in {
    val ch = new EmbeddedChannel(new MessageHandler(ServerContext(null, new NonEmptyUserAuthorizer, None)))

    val cmd = new Hello(ProtocolVersion.value, "3.2.1", 1234567L, Map.empty)
    val frame = cmd.toFrame[ByteBuf]

    ch.writeInbound(frame)
    val respFrame = ch.readOutbound[Frame]()
    respFrame.frameType shouldEqual Tags.HELLO_RESPONSE
    val credentialsFrame = ch.readOutbound[Frame]()
    credentialsFrame.frameType shouldEqual Tags.CREDENTIALS_REQUEST

    ch.writeInbound(Credentials(CredentialsRequest.METHOD_PLAIN, "", "").toFrame[ByteBuf])
    ch.finish() shouldBe true

    val errFrame = ch.readOutbound[Frame]()
    ch.isOpen shouldBe false
    errFrame.frameType shouldEqual Tags.ERROR_MESSAGE
    val err = ErrorMessage.readFrame(errFrame)
    err.message shouldEqual "Username should not be empty"
    err.severity shouldEqual ErrorMessage.SEVERITY_FATAL
  }

  it should "handle query" in {
    import org.yupana.core.sql.parser
    val queryEngine = mock[QueryEngineRouter]
    val ch = new EmbeddedChannel(new MessageHandler(ServerContext(queryEngine, new NonEmptyUserAuthorizer, None)))
    auth(ch)

    (queryEngine.query _)
      .expects(
        "SELECT ? + ? as five, ? as s, ? epoch",
        Map(
          1 -> parser.NumericValue(3),
          2 -> parser.NumericValue(2),
          3 -> parser.StringValue("str"),
          4 -> parser.TimestampValue(0L)
        )
      )
      .returning(
        Right(
          SimpleResult(
            "result",
            Seq("five", "s", "epoch"),
            Seq(DataType[Int], DataType[String], DataType[Time]),
            Iterator(Array(5, "abc", Time(0L)))
          )
        )
      )

    When("Query is sent")
    ch.writeInbound(
      PrepareQuery(
        "SELECT ? + ? as five, ? as s, ? epoch",
        Map(1 -> NumericValue(3), 2 -> NumericValue(2), 3 -> StringValue("str"), 4 -> TimestampValue(0L))
      ).toFrame[ByteBuf]
    )

    Then("Sent header as reply")
    val header = readMessage(ch, ResultHeader)
    header.fields should contain theSameElementsInOrderAs List(
      ResultField("five", "INTEGER"),
      ResultField("s", "VARCHAR"),
      ResultField("epoch", "TIMESTAMP")
    )

    When("client request for batch")
    ch.writeInbound(Next(10).toFrame[ByteBuf])

    Then("reply with actual data")
    val row = readMessage(ch, ResultRow)
    row.values should contain theSameElementsInOrderAs List(
      Array(5),
      Array(0, 0, 0, 3) ++ "abc".getBytes(),
      Array(0)
    )

    Then("reply with footer")
    val footer = readMessage(ch, ResultFooter)
    footer.rows shouldEqual 1
  }

  private def readMessage[T <: Message[T]](ch: EmbeddedChannel, messageHelper: MessageHelper[T]): T = {
    val frame = ch.readOutbound[Frame]()
    frame.frameType shouldEqual messageHelper.tag
    messageHelper.readFrame(frame)
  }

  private def auth(ch: EmbeddedChannel): Unit = {
    val cmd = new Hello(ProtocolVersion.value, "3.2.1", 1234567L, Map.empty)
    val frame = cmd.toFrame[ByteBuf]

    ch.writeInbound(frame)
    val respFrame = ch.readOutbound[Frame]()
    respFrame.frameType shouldEqual Tags.HELLO_RESPONSE
    val credentialsFrame = ch.readOutbound[Frame]()
    credentialsFrame.frameType shouldEqual Tags.CREDENTIALS_REQUEST

    ch.writeInbound(Credentials(CredentialsRequest.METHOD_PLAIN, "test", "").toFrame[ByteBuf])

    val authFrame = ch.readOutbound[Frame]()
    authFrame.frameType shouldEqual Tags.AUTHORIZED
    val idleFrame = ch.readOutbound[Frame]()
    idleFrame.frameType shouldEqual Tags.IDLE
  }
}
