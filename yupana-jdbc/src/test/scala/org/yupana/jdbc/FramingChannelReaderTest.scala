package org.yupana.jdbc

import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.{ AsynchronousByteChannel, CompletionHandler }
import java.util.concurrent.{ CompletableFuture, Future }

class SimpleAsyncByteChannel(data: Array[Byte]) extends AsynchronousByteChannel {

  private var closed = false
  override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, _ >: A]): Unit = {
    dst.put(data)
    handler.completed(data.length, attachment)
  }

  override def read(dst: ByteBuffer): Future[Integer] = {
    dst.put(data)
    CompletableFuture.completedFuture(data.length)
  }

  override def write[A](src: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, _ >: A]): Unit = {
    handler.completed(src.remaining(), attachment)
  }

  override def write(src: ByteBuffer): Future[Integer] = {
    CompletableFuture.completedFuture(src.remaining())
  }

  override def close(): Unit = closed = true

  override def isOpen: Boolean = !closed
}

class FramingChannelReaderTest extends AnyFlatSpec with Matchers with MockFactory with OptionValues {

  "FramingChannelReader" should "read frame from channel" in {
    val channel = new SimpleAsyncByteChannel(createFrame(1, 2, 3, 4, 5))
    val reader = new FramingChannelReader(channel, 100)

    val frame = reader.awaitAndReadFrame()
    frame.frameType shouldEqual 42
    frame.payload should contain theSameElementsAs Array(1, 2, 3, 4, 5)
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

    (channel
      .read(_: ByteBuffer))
      .expects(*)
      .onCall((_: ByteBuffer) => CompletableFuture.completedFuture(Integer.valueOf(-1)))

    an[IOException] should be thrownBy iterator.awaitAndReadFrame()
  }

  it should "return None when it cannot read frame size" in {
    val channel = new SimpleAsyncByteChannel(Array(1, 2, 3))
    val reader = new FramingChannelReader(channel, 100)

    reader.awaitAndReadFrame() shouldBe None
  }

  it should "continue read several frames from channel" in {
    val channel = new SimpleAsyncByteChannel(
      createFrame(1, 2, 3) ++ createFrame(4, 5, 6, 7) ++ createFrame(8, 9)
    )

    val reader = new FramingChannelReader(channel, 100)

    reader.awaitAndReadFrame().payload should contain theSameElementsAs Array(1, 2, 3)
    reader.awaitAndReadFrame().payload should contain theSameElementsAs Array(4, 5, 6, 7)
    reader.awaitAndReadFrame().payload should contain theSameElementsAs Array(8, 9)
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

    (channel.read(_: ByteBuffer)).expects(*).onCall { (bb: ByteBuffer) =>
      bb.put(Array[Byte](6, 7))
      CompletableFuture.completedFuture(Integer.valueOf(2))
    }

    iterator.awaitAndReadFrame().payload should contain theSameElementsInOrderAs Array[Byte](1, 2, 3, 6, 7)
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
    frame1.payload should contain theSameElementsInOrderAs Array[Byte](1, 2, 3, 4)

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
      .read(_: ByteBuffer))
      .expects(*)
      .onCall { (bb: ByteBuffer) =>
        bb.put(Array[Byte](7, 8, 9))
        CompletableFuture.completedFuture(Integer.valueOf(3))
      }
      .once()

    val frame2 = reader.awaitAndReadFrame()
    frame2.frameType shouldEqual 2
    frame2.payload should contain theSameElementsInOrderAs Array[Byte](5, 6, 7, 8, 9)
  }

  private def createFrame(data: Byte*): Array[Byte] = {
    val bb = ByteBuffer.allocate(data.length + 5)
    bb.put(42.toByte)
    bb.putInt(data.length)
    bb.put(data.toArray)
    bb.array()
  }
}
