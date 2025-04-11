package org.yupana.jdbc

import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.{ AsynchronousByteChannel, CompletionHandler }

class FramingChannelReaderTest extends AnyFlatSpec with Matchers with MockFactory with OptionValues {

  "FramingChannelReader" should "read frame from channel" in {
    val channel = new SimpleAsyncByteChannel(createFrame(1, 2, 3, 4, 5))
    val reader = new FramingChannelReader(channel, 100)

    val frame = reader.awaitAndReadFrame()
    frame.frameType shouldEqual 42
    frame.payload.array() should contain theSameElementsAs Array(1, 2, 3, 4, 5)
  }

  it should "throw an exception when channel is closed unexpectedly" in {
    val channel = mock[AsynchronousByteChannel]
    val iterator = new FramingChannelReader(channel, 100)

    (channel
      .read[Any](_: ByteBuffer, _: Any, _: CompletionHandler[Integer, _ >: Any]))
      .expects(*, *, *)
      .onCall { (bb, a, h) =>
        bb.put(5.toByte)
        bb.putInt(10)
        bb.put(Array[Byte](1, 2, 3, 4, 5))
        h.completed(10, a)
      }

    (channel.read[Any](_: ByteBuffer, _: Any, _: CompletionHandler[Integer, _ >: Any])).expects(*, *, *).onCall {
      (_, a, h) => h.completed(-1, a)
    }

    an[IOException] should be thrownBy iterator.awaitAndReadFrame()
  }

  it should "throw an exception when it cannot read frame size" in {
    val channel = new SimpleAsyncByteChannel(Array(1, 2, 3))
    val reader = new FramingChannelReader(channel, 100)

    an[IOException] should be thrownBy reader.awaitAndReadFrame()
  }

  it should "continue read several frames from channel" in {
    val channel = new SimpleAsyncByteChannel(
      createFrame(1, 2, 3) ++ createFrame(4, 5, 6, 7) ++ createFrame(8, 9)
    )

    val reader = new FramingChannelReader(channel, 100)

    reader.awaitAndReadFrame().payload.array() should contain theSameElementsAs Array(1, 2, 3)
    reader.awaitAndReadFrame().payload.array() should contain theSameElementsAs Array(4, 5, 6, 7)
    reader.awaitAndReadFrame().payload.array() should contain theSameElementsAs Array(8, 9)
  }

  it should "handle create a single chunk from parts" in {
    val channel = mock[AsynchronousByteChannel]

    val iterator = new FramingChannelReader(channel, 100)

    (channel.read[Any](_: ByteBuffer, _: Any, _: CompletionHandler[Integer, _ >: Any])).expects(*, *, *).onCall {
      (bb, a, h) =>
        bb.put(33.toByte)
        bb.putInt(5)
        bb.put(Array[Byte](1, 2, 3))
        h.completed(8, a)
    }

    (channel.read[Any](_: ByteBuffer, _: Any, _: CompletionHandler[Integer, _ >: Any])).expects(*, *, *).onCall {
      (bb, a, h) =>
        bb.put(Array[Byte](6, 7))
        h.completed(2, a)
    }

    iterator.awaitAndReadFrame().payload.array() should contain theSameElementsInOrderAs Array[Byte](1, 2, 3, 6, 7)
  }

  it should "handle buffer overflow" in {
    val channel = mock[AsynchronousByteChannel]

    val reader = new FramingChannelReader(channel, 14)

    (channel
      .read[Any](_: ByteBuffer, _: Any, _: CompletionHandler[Integer, _ >: Any]))
      .expects(*, *, *)
      .onCall { (bb, a, h) =>
        bb.put(1.toByte)
        bb.putInt(4)
        bb.put(Array[Byte](1, 2, 3, 4))
        h.completed(9, a)
      }
      .once()

    val frame1 = reader.awaitAndReadFrame()
    frame1.frameType shouldEqual 1
    frame1.payload.array() should contain theSameElementsInOrderAs Array[Byte](1, 2, 3, 4)

    (channel
      .read[Any](_: ByteBuffer, _: Any, _: CompletionHandler[Integer, _ >: Any]))
      .expects(*, *, *)
      .onCall { (bb, a, h) =>
        bb.put(2.toByte)
        bb.putInt(5)
        bb.put(Array[Byte](5, 6))
        h.completed(7, a)
      }
      .once()

    (channel
      .read[Any](_: ByteBuffer, _: Any, _: CompletionHandler[Integer, _ >: Any]))
      .expects(*, *, *)
      .onCall { (bb, a, h) =>
        bb.put(Array[Byte](7, 8, 9))
        h.completed(3, a)
      }
      .once()

    val frame2 = reader.awaitAndReadFrame()
    frame2.frameType shouldEqual 2
    frame2.payload.array() should contain theSameElementsInOrderAs Array[Byte](5, 6, 7, 8, 9)
  }

  private def createFrame(data: Byte*): Array[Byte] = {
    val bb = ByteBuffer.allocate(data.length + 5)
    bb.put(42.toByte)
    bb.putInt(data.length)
    bb.put(data.toArray)
    bb.array()
  }
}
