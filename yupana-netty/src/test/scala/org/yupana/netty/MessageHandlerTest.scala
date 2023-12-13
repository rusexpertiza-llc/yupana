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
import org.yupana.core.auth.YupanaUser
import org.yupana.core.sql.parser
import org.yupana.protocol._

class MessageHandlerTest extends AnyFlatSpec with Matchers with GivenWhenThen with MockFactory {

  import NettyBuffer._

  "ConnectingHandler" should "establish connection" in {
    Given("Message Handler")
    val ch = new EmbeddedChannel(new ConnectingHandler(ServerContext(null, new NonEmptyUserAuthorizer)))

    val cmd = new Hello(ProtocolVersion.value, "3.2.1", 1234567L, Map.empty)
    val frame = cmd.toFrame[ByteBuf]

    When("Hello command received")
    ch.writeInbound(frame) //  shouldBe true

    Then("Hello response and credentials req should be replied")
    val resp = readMessage(ch, HelloResponse)
    resp.protocolVersion shouldBe ProtocolVersion.value
    resp.reqTime shouldEqual 1234567L

    val credentials = readMessage(ch, CredentialsRequest)
    credentials.methods shouldEqual Seq(CredentialsRequest.METHOD_PLAIN)

    When("Credentials sent")

    ch.writeInbound(Credentials(CredentialsRequest.METHOD_PLAIN, "test", "pass").toFrame[ByteBuf])
    ch.finish() shouldBe true

    Then("Authorized should be replied")
    val authFrame = ch.readOutbound[Frame]()
    authFrame.frameType shouldEqual Tags.AUTHORIZED
  }

  it should "fail on unknown auth method" in {
    val ch = new EmbeddedChannel(new ConnectingHandler(ServerContext(null, new NonEmptyUserAuthorizer)))

    val cmd = new Hello(ProtocolVersion.value, "3.2.1", 1234567L, Map.empty)
    val frame = cmd.toFrame[ByteBuf]

    ch.writeInbound(frame)
    val respFrame = ch.readOutbound[Frame]()
    respFrame.frameType shouldEqual Tags.HELLO_RESPONSE
    val credentialsFrame = ch.readOutbound[Frame]()
    credentialsFrame.frameType shouldEqual Tags.CREDENTIALS_REQUEST

    ch.writeInbound(Credentials("SECURE", "test", "pass").toFrame[ByteBuf])
    ch.finish() shouldBe true

    val err = readMessage(ch, ErrorMessage)
    ch.isOpen shouldBe false
    err.message shouldEqual "Unsupported auth method 'SECURE'"
    err.severity shouldEqual ErrorMessage.SEVERITY_FATAL
  }

  it should "fail with empty user" in {
    val ch = new EmbeddedChannel(new ConnectingHandler(ServerContext(null, new NonEmptyUserAuthorizer)))

    val cmd = new Hello(ProtocolVersion.value, "3.2.1", 1234567L, Map.empty)
    val frame = cmd.toFrame[ByteBuf]

    ch.writeInbound(frame)
    val respFrame = ch.readOutbound[Frame]()
    respFrame.frameType shouldEqual Tags.HELLO_RESPONSE
    val credentialsFrame = ch.readOutbound[Frame]()
    credentialsFrame.frameType shouldEqual Tags.CREDENTIALS_REQUEST

    ch.writeInbound(Credentials(CredentialsRequest.METHOD_PLAIN, "", "").toFrame[ByteBuf])
    ch.finish() shouldBe true

    val err = readMessage(ch, ErrorMessage)
    ch.isOpen shouldBe false
    err.message shouldEqual "Username should not be empty"
    err.severity shouldEqual ErrorMessage.SEVERITY_FATAL
  }

  it should "handle query after auth" in {
    val queryEngine = mock[QueryEngineRouter]
    val ch = new EmbeddedChannel(new ConnectingHandler(ServerContext(queryEngine, new NonEmptyUserAuthorizer)))
    auth(ch)

    (queryEngine.query _)
      .expects(
        YupanaUser("test"),
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
            Iterator(Array[Any](5, "abc", Time(0L)))
          )
        )
      )

    When("Query is sent")
    ch.writeInbound(
      SqlQuery(
        11,
        "SELECT ? + ? as five, ? as s, ? epoch",
        Map(1 -> NumericValue(3), 2 -> NumericValue(2), 3 -> StringValue("str"), 4 -> TimestampValue(0L))
      ).toFrame[ByteBuf]
    )

    Then("Sent header as reply")
    val header = readMessage(ch, ResultHeader)
    header.id shouldEqual 11
    header.fields should contain theSameElementsInOrderAs List(
      ResultField("five", "INTEGER"),
      ResultField("s", "VARCHAR"),
      ResultField("epoch", "TIMESTAMP")
    )

    When("client request for batch")
    ch.writeInbound(Next(11, 10).toFrame[ByteBuf])

    Then("reply with actual data")
    val row = readMessage(ch, ResultRow)
    row.id shouldEqual 11
    row.values should contain theSameElementsInOrderAs List(
      Array(5),
      Array(0, 0, 0, 3) ++ "abc".getBytes(),
      Array(0)
    )

    Then("reply with footer")
    val footer = readMessage(ch, ResultFooter)
    footer.id shouldEqual 11
    footer.rows shouldEqual 1
  }

  "QueryHandler" should "handle multiple queries simultaneously" in {
    val queryEngine = mock[QueryEngineRouter]
    val ch = new EmbeddedChannel(new QueryHandler(ServerContext(queryEngine, new NonEmptyUserAuthorizer), "test"))

    (queryEngine.query _)
      .expects(YupanaUser("test"), "SELECT 1", Map.empty[Int, parser.Value])
      .returning(Right(SimpleResult("result 1", Seq("1"), Seq(DataType[Int]), Iterator(Array[Any](1)))))

    (queryEngine.query _)
      .expects(YupanaUser("test"), "SELECT 2", Map.empty[Int, parser.Value])
      .returning(Right(SimpleResult("result 2", Seq("2"), Seq(DataType[Int]), Iterator(Array[Any](2)))))

    ch.writeInbound(SqlQuery(1, "SELECT 1", Map.empty).toFrame)
    ch.writeInbound(SqlQuery(2, "SELECT 2", Map.empty).toFrame)

    val header1 = readMessage(ch, ResultHeader)
    header1.id shouldEqual 1
    header1.tableName shouldEqual "result 1"
    header1.fields should contain theSameElementsInOrderAs List(
      ResultField("1", "INTEGER")
    )

    val header2 = readMessage(ch, ResultHeader)
    header2.id shouldEqual 2
    header2.tableName shouldEqual "result 2"
    header2.fields should contain theSameElementsInOrderAs List(
      ResultField("2", "INTEGER")
    )

    ch.writeInbound(Next(2, 10).toFrame)
    val data2 = readMessage(ch, ResultRow)
    data2.id shouldEqual 2
    data2.values should contain theSameElementsInOrderAs List(Array(2))

    val footer2 = readMessage(ch, ResultFooter)
    footer2.id shouldEqual 2

    ch.writeInbound(Next(1, 10).toFrame)
    val data1 = readMessage(ch, ResultRow)
    data1.id shouldEqual 1
    data1.values should contain theSameElementsInOrderAs List(Array(1))

    val footer1 = readMessage(ch, ResultFooter)
    footer1.id shouldEqual 1

    ch.writeInbound(Next(1, 10).toFrame)
    val err = readMessage(ch, ErrorMessage)
    err.message shouldEqual "Unknown stream id 1"
    err.streamId shouldEqual Some(1)
  }

  it should "should handle cancel queries" in {
    val queryEngine = mock[QueryEngineRouter]
    val ch = new EmbeddedChannel(new QueryHandler(ServerContext(queryEngine, new NonEmptyUserAuthorizer), "Test"))

    (queryEngine.query _)
      .expects(YupanaUser("Test"), "SELECT 1", Map.empty[Int, parser.Value])
      .returning(Right(SimpleResult("result 1", Seq("1"), Seq(DataType[Int]), Iterator(Array[Any](1)))))

    ch.writeInbound(SqlQuery(1, "SELECT 1", Map.empty).toFrame)

    val header = readMessage(ch, ResultHeader)
    header.id shouldEqual 1
    header.tableName shouldEqual "result 1"

    ch.writeInbound(Cancel(1).toFrame)
    readMessage(ch, Canceled).id shouldEqual 1

    ch.writeInbound(Next(1, 10).toFrame)
    val err = readMessage(ch, ErrorMessage)
    err.message shouldEqual "Unknown stream id 1"
    err.streamId shouldEqual Some(1)
  }

  it should "handle errors in backend" in {
    val queryEngine = mock[QueryEngineRouter]
    val ch = new EmbeddedChannel(
      new QueryHandler(ServerContext(queryEngine, new NonEmptyUserAuthorizer), "test")
    )

    (queryEngine.query _)
      .expects(YupanaUser("test"), "SELECT 2", Map.empty[Int, parser.Value])
      .onCall(_ => throw new RuntimeException("Something wrong"))

    ch.writeInbound(SqlQuery(1, "SELECT 2", Map.empty).toFrame)

    val err = readMessage(ch, ErrorMessage)
    err.message should include("Something wrong")
  }

  private def readMessage[T <: Message[T]](ch: EmbeddedChannel, messageHelper: MessageHelper[T]): T = {
    val frame = ch.readOutbound[Frame]()
    if (frame.frameType == Tags.ERROR_MESSAGE && messageHelper.tag != Tags.ERROR_MESSAGE) {
      val em = ErrorMessage.readFrame(frame)
      fail(em.message)
    }
    frame.frameType.toChar shouldEqual messageHelper.tag.toChar
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
  }
}
