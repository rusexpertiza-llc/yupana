package org.yupana.netty

import io.netty.buffer.{ ByteBuf, Unpooled }
import io.netty.channel.embedded.EmbeddedChannel
import org.scalamock.scalatest.MockFactory
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.yupana.api.Time
import org.yupana.api.query.SimpleResult
import org.yupana.api.types.{ ByteReaderWriter, DataType, SimpleStringReaderWriter, StringReaderWriter }
import org.yupana.core.QueryEngineRouter
import org.yupana.core.auth.{ NonEmptyUserAuthorizer, TsdbRole, YupanaUser }
import org.yupana.core.sql.parser
import org.yupana.core.sql.parser.Value
import org.yupana.protocol._

class MessageHandlerTest extends AnyFlatSpec with Matchers with GivenWhenThen with MockFactory {

  implicit val rw: ByteReaderWriter[ByteBuf] = ByteBufEvalReaderWriter
  implicit val srw: StringReaderWriter = SimpleStringReaderWriter

  "ConnectingHandler" should "establish connection" in {
    Given("Message Handler")
    val ch = new EmbeddedChannel(new ConnectingHandler(ServerContext(null, NonEmptyUserAuthorizer)))

    val cmd = new Hello(ProtocolVersion.value, "3.2.1", 1234567L, Map.empty)
    val frame = cmd.toFrame(Unpooled.buffer())

    When("Hello command received")
    ch.writeInbound(frame) //  shouldBe true

    Then("Hello response and credentials req should be replied")
    val resp = readMessage(ch, HelloResponse)
    resp.protocolVersion shouldBe ProtocolVersion.value
    resp.reqTime shouldEqual 1234567L

    val credentials = readMessage(ch, CredentialsRequest)
    credentials.methods shouldEqual Seq(CredentialsRequest.METHOD_PLAIN)

    When("Credentials sent")

    ch.writeInbound(Credentials(CredentialsRequest.METHOD_PLAIN, Some("test"), Some("pass")).toFrame(Unpooled.buffer()))
    ch.finish() shouldBe true

    Then("Authorized should be replied")
    val authFrame = ch.readOutbound[Frame[ByteBuf]]()
    authFrame.frameType shouldEqual Tags.AUTHORIZED.value
  }

  it should "check protocol version" in {
    val ch = new EmbeddedChannel(new ConnectingHandler(ServerContext(null, NonEmptyUserAuthorizer)))

    val cmd = new Hello(ProtocolVersion.value + 5, "3.2.1", 1234567L, Map.empty)
    ch.writeInbound(cmd.toFrame(Unpooled.buffer()))
    val em = readMessage(ch, ErrorMessage)

    em.message shouldEqual s"Unsupported protocol version ${ProtocolVersion.value + 5}, required ${ProtocolVersion.value}"
    em.severity shouldEqual ErrorMessage.SEVERITY_FATAL
  }

  it should "fail on unknown auth method" in {
    val ch = new EmbeddedChannel(new ConnectingHandler(ServerContext(null, NonEmptyUserAuthorizer)))

    val cmd = new Hello(ProtocolVersion.value, "3.2.1", 1234567L, Map.empty)
    val frame = cmd.toFrame(Unpooled.buffer())

    ch.writeInbound(frame)
    val respFrame = ch.readOutbound[Frame[ByteBuf]]()
    respFrame.frameType shouldEqual Tags.HELLO_RESPONSE.value
    val credentialsFrame = ch.readOutbound[Frame[ByteBuf]]()
    credentialsFrame.frameType shouldEqual Tags.CREDENTIALS_REQUEST.value

    ch.writeInbound(Credentials("SECURE", Some("test"), Some("pass")).toFrame(Unpooled.buffer()))
    ch.finish() shouldBe true

    val err = readMessage(ch, ErrorMessage)
    ch.isOpen shouldBe false
    err.message shouldEqual "Unsupported auth method 'SECURE'"
    err.severity shouldEqual ErrorMessage.SEVERITY_FATAL
  }

  it should "fail with empty user" in {
    val ch = new EmbeddedChannel(new ConnectingHandler(ServerContext(null, NonEmptyUserAuthorizer)))

    val cmd = new Hello(ProtocolVersion.value, "3.2.1", 1234567L, Map.empty)
    val frame = cmd.toFrame(Unpooled.buffer())

    ch.writeInbound(frame)
    val respFrame = ch.readOutbound[Frame[ByteBuf]]()
    respFrame.frameType shouldEqual Tags.HELLO_RESPONSE.value
    val credentialsFrame = ch.readOutbound[Frame[ByteBuf]]()
    credentialsFrame.frameType shouldEqual Tags.CREDENTIALS_REQUEST.value

    ch.writeInbound(Credentials(CredentialsRequest.METHOD_PLAIN, None, None).toFrame(Unpooled.buffer()))
    ch.finish() shouldBe true

    val err = readMessage(ch, ErrorMessage)
    ch.isOpen shouldBe false
    err.message shouldEqual "Username should not be empty"
    err.severity shouldEqual ErrorMessage.SEVERITY_FATAL
  }

  it should "handle query after auth" in {
    val queryEngine = mock[QueryEngineRouter]
    val ch = new EmbeddedChannel(new ConnectingHandler(ServerContext(queryEngine, NonEmptyUserAuthorizer)))
    auth(ch)

    (queryEngine
      .query(_: YupanaUser, _: String, _: Map[Int, Value])(_: StringReaderWriter))
      .expects(
        YupanaUser("test", None, TsdbRole.Admin),
        "SELECT ? + ? as five, ? as s, ? epoch",
        Map(
          1 -> parser.TypedValue(BigDecimal(3)),
          2 -> parser.TypedValue(BigDecimal(2)),
          3 -> parser.TypedValue("str"),
          4 -> parser.TypedValue(Time(0L))
        ),
        *
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
      ).toFrame(Unpooled.buffer())
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
    ch.writeInbound(NextBatch(11, 10).toFrame(Unpooled.buffer()))

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
    val user = YupanaUser("test", None, TsdbRole.Admin)
    val ch = new EmbeddedChannel(new QueryHandler(ServerContext(queryEngine, NonEmptyUserAuthorizer), user))

    (queryEngine
      .query(_: YupanaUser, _: String, _: Map[Int, Value])(_: StringReaderWriter))
      .expects(user, "SELECT 1", Map.empty[Int, parser.Value], *)
      .returning(Right(SimpleResult("result 1", Seq("1"), Seq(DataType[Int]), Iterator(Array[Any](1)))))

    (queryEngine
      .query(_: YupanaUser, _: String, _: Map[Int, Value])(_: StringReaderWriter))
      .expects(user, "SELECT 2", Map.empty[Int, parser.Value], *)
      .returning(Right(SimpleResult("result 2", Seq("2"), Seq(DataType[Int]), Iterator(Array[Any](2)))))

    ch.writeInbound(SqlQuery(1, "SELECT 1", Map.empty).toFrame(Unpooled.buffer()))
    ch.writeInbound(SqlQuery(2, "SELECT 2", Map.empty).toFrame(Unpooled.buffer()))

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

    ch.writeInbound(NextBatch(2, 10).toFrame(Unpooled.buffer()))
    val data2 = readMessage(ch, ResultRow)
    data2.id shouldEqual 2
    data2.values should contain theSameElementsInOrderAs List(Array(2))

    val footer2 = readMessage(ch, ResultFooter)
    footer2.id shouldEqual 2

    ch.writeInbound(NextBatch(1, 10).toFrame(Unpooled.buffer()))
    val data1 = readMessage(ch, ResultRow)
    data1.id shouldEqual 1
    data1.values should contain theSameElementsInOrderAs List(Array(1))

    val footer1 = readMessage(ch, ResultFooter)
    footer1.id shouldEqual 1

    ch.writeInbound(NextBatch(1, 10).toFrame(Unpooled.buffer()))
    val err = readMessage(ch, ErrorMessage)
    err.message shouldEqual "Unknown stream id 1"
    err.streamId shouldEqual Some(1)
  }

  it should "send as many rows as acquired" in {
    val queryEngine = mock[QueryEngineRouter]
    val user = YupanaUser("test", None, TsdbRole.Admin)
    val ch = new EmbeddedChannel(new QueryHandler(ServerContext(queryEngine, NonEmptyUserAuthorizer), user))

    (queryEngine
      .query(_: YupanaUser, _: String, _: Map[Int, Value])(_: StringReaderWriter))
      .expects(user, "SELECT x FROM table", Map.empty[Int, parser.Value], *)
      .returning(Right(SimpleResult("table", Seq("x"), Seq(DataType[Int]), (1 to 15).map(x => Array[Any](x)).iterator)))

    ch.writeInbound(SqlQuery(1, "SELECT x FROM table", Map.empty).toFrame(Unpooled.buffer()))
    val header = readMessage(ch, ResultHeader)
    header.tableName shouldEqual "table"
    header.id shouldEqual 1

    ch.writeInbound(NextBatch(1, 10).toFrame(Unpooled.buffer()))
    (1 to 10).foreach { e =>
      val row = readMessage(ch, ResultRow)
      row.id shouldEqual 1
      row.values(0) shouldEqual Array(e)
    }
    ch.readOutbound[Frame[ByteBuf]]() shouldBe null

    ch.writeInbound(NextBatch(1, 10).toFrame(Unpooled.buffer()))
    (11 to 15).foreach { e =>
      val row = readMessage(ch, ResultRow)
      row.id shouldEqual 1
      row.values(0) shouldEqual Array(e)
    }
    val ftr = readMessage(ch, ResultFooter)
    ftr.rows shouldEqual 15
  }

  it should "support batch queries" in {
    val queryEngine = mock[QueryEngineRouter]
    val user = YupanaUser("test", None, TsdbRole.Admin)
    val ch = new EmbeddedChannel(new QueryHandler(ServerContext(queryEngine, NonEmptyUserAuthorizer), user))

    (queryEngine
      .batchQuery(_: YupanaUser, _: String, _: Seq[Map[Int, Value]])(_: StringReaderWriter))
      .expects(
        user,
        "UPSERT INTO test(a,b) VALUES (?, ?)",
        Seq(
          Map(1 -> parser.TypedValue("a"), 2 -> parser.TypedValue(BigDecimal(5))),
          Map(1 -> parser.TypedValue("b"), 2 -> parser.TypedValue(BigDecimal(7)))
        ),
        *
      )
      .returning(Right(SimpleResult("result", Seq("Status"), Seq(DataType[String]), Iterator(Array[Any]("OK")))))

    ch.writeInbound(
      BatchQuery(
        1,
        "UPSERT INTO test(a,b) VALUES (?, ?)",
        Seq(
          Map[Int, ParameterValue](1 -> StringValue("a"), 2 -> NumericValue(5)),
          Map[Int, ParameterValue](1 -> StringValue("b"), 2 -> NumericValue(7))
        )
      ).toFrame(Unpooled.buffer())
    )

    val hdr = readMessage(ch, ResultHeader)
    hdr.tableName shouldEqual "result"
  }

  it should "should handle cancel queries" in {
    val queryEngine = mock[QueryEngineRouter]
    val user = YupanaUser("Test", None, TsdbRole.ReadWrite)
    val ch = new EmbeddedChannel(new QueryHandler(ServerContext(queryEngine, NonEmptyUserAuthorizer), user))

    (queryEngine
      .query(_: YupanaUser, _: String, _: Map[Int, Value])(_: StringReaderWriter))
      .expects(user, "SELECT 1", Map.empty[Int, parser.Value], *)
      .returning(Right(SimpleResult("result 1", Seq("1"), Seq(DataType[Int]), Iterator(Array[Any](1)))))

    ch.writeInbound(SqlQuery(1, "SELECT 1", Map.empty).toFrame(Unpooled.buffer()))

    val header = readMessage(ch, ResultHeader)
    header.id shouldEqual 1
    header.tableName shouldEqual "result 1"

    ch.writeInbound(Cancel(1).toFrame(Unpooled.buffer()))
    readMessage(ch, Canceled).id shouldEqual 1

    ch.writeInbound(NextBatch(1, 10).toFrame(Unpooled.buffer()))
    val err = readMessage(ch, ErrorMessage)
    err.message shouldEqual "Unknown stream id 1"
    err.streamId shouldEqual Some(1)
  }

  it should "handle errors in backend" in {
    val queryEngine = mock[QueryEngineRouter]
    val user = YupanaUser("test", None, TsdbRole.Admin)
    val ch = new EmbeddedChannel(new QueryHandler(ServerContext(queryEngine, NonEmptyUserAuthorizer), user))

    (queryEngine
      .query(_: YupanaUser, _: String, _: Map[Int, Value])(_: StringReaderWriter))
      .expects(user, "SELECT 2", Map.empty[Int, parser.Value], *)
      .onCall(_ => throw new RuntimeException("Something wrong"))

    ch.writeInbound(SqlQuery(1, "SELECT 2", Map.empty).toFrame(Unpooled.buffer()))

    val err = readMessage(ch, ErrorMessage)
    err.message should include("Something wrong")
  }

  private def readMessage[T <: Message[T]](ch: EmbeddedChannel, messageHelper: MessageHelper[T]): T = {
    val frame = ch.readOutbound[Frame[ByteBuf]]()
    if (frame.frameType == Tags.ERROR_MESSAGE.value && messageHelper.tag != Tags.ERROR_MESSAGE) {
      val em = ErrorMessage.readFrame(frame)
      fail(em.message)
    }
    frame.frameType.toChar shouldEqual messageHelper.tag.value.toChar
    messageHelper.readFrame(frame)
  }

  private def auth(ch: EmbeddedChannel): Unit = {
    val cmd = new Hello(ProtocolVersion.value, "3.2.1", 1234567L, Map.empty)
    val frame = cmd.toFrame(Unpooled.buffer())

    ch.writeInbound(frame)
    val respFrame = ch.readOutbound[Frame[ByteBuf]]()
    respFrame.frameType shouldEqual Tags.HELLO_RESPONSE.value
    val credentialsFrame = ch.readOutbound[Frame[ByteBuf]]()
    credentialsFrame.frameType shouldEqual Tags.CREDENTIALS_REQUEST.value

    ch.writeInbound(Credentials(CredentialsRequest.METHOD_PLAIN, Some("test"), None).toFrame(Unpooled.buffer()))

    val authFrame = ch.readOutbound[Frame[ByteBuf]]()
    authFrame.frameType shouldEqual Tags.AUTHORIZED.value
  }
}
