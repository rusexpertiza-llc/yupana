package org.yupana.jdbc

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.channels.{ Channels, ReadableByteChannel }

import org.scalamock.scalatest.MockFactory
import org.scalatest.{ FlatSpec, Matchers }

class FramingChannelIteratorTest extends FlatSpec with Matchers with MockFactory {

  "FramingChannelIterator" should "read frame from channel" in {
    val channel = Channels.newChannel(new ByteArrayInputStream(createFrame(1, 2, 3, 4, 5)))
    val iterator = new FramingChannelIterator(channel, 100)

    iterator.hasNext shouldBe true
    val bytes = iterator.next()
    bytes should contain theSameElementsAs Array(1, 2, 3, 4, 5)

    iterator.hasNext shouldBe false
  }

  it should "not allow reading on empty iterator" in {
    val channel = Channels.newChannel(new ByteArrayInputStream(createFrame(42)))
    val iterator = new FramingChannelIterator(channel, 200)

    iterator.hasNext shouldBe true
    val bytes = iterator.next()
    bytes should contain theSameElementsAs Array(42)

    an[IllegalStateException] should be thrownBy iterator.next()
  }

  it should "throw an exception when channel is closed unexpectedly" in {
    val channel = mock[ReadableByteChannel]
    val iterator = new FramingChannelIterator(channel, 100)

    (channel.isOpen _).expects().returning(true).anyNumberOfTimes()
    (channel.read _).expects(*).onCall { bb: ByteBuffer =>
      bb.putInt(10)
      bb.put(Array[Byte](1, 2, 3, 4, 5))
      9
    }

    (channel.read _).expects(*).onCall((_: ByteBuffer) => -1)

    an[IllegalStateException] should be thrownBy iterator.next()
  }

  it should "throw an exception when it cannot read frame size" in {
    val channel = Channels.newChannel(new ByteArrayInputStream(Array(1, 2, 3)))
    val iterator = new FramingChannelIterator(channel, 100)
    iterator.hasNext shouldBe true

    an[IllegalStateException] should be thrownBy iterator.next()
  }

  it should "continue read several frames from channel" in {
    val channel = Channels.newChannel(
      new ByteArrayInputStream(
        createFrame(1, 2, 3) ++ createFrame(4, 5, 6, 7) ++ createFrame(8, 9)
      )
    )
    val iterator = new FramingChannelIterator(channel, 100)

    iterator.next() should contain theSameElementsAs Array(1, 2, 3)
    iterator.next() should contain theSameElementsAs Array(4, 5, 6, 7)
    iterator.next() should contain theSameElementsAs Array(8, 9)
  }

  it should "handle create a single chunk from parts" in {
    val channel = mock[ReadableByteChannel]

    val iterator = new FramingChannelIterator(channel, 100)

    (channel.isOpen _).expects().returning(true).anyNumberOfTimes()
    (channel.read _).expects(*).onCall { bb: ByteBuffer =>
      bb.putInt(5)
      bb.put(Array[Byte](1, 2, 3))
      7
    }

    (channel.read _).expects(*).onCall { bb: ByteBuffer =>
      bb.put(Array[Byte](6, 7))
      2
    }

    iterator.next() should contain theSameElementsInOrderAs Array[Byte](1, 2, 3, 6, 7)
  }

  it should "handle buffer overflow" in {
    val channel = mock[ReadableByteChannel]

    val iterator = new FramingChannelIterator(channel, 14)

    (channel.isOpen _).expects().returning(true).anyNumberOfTimes()
    (channel.read _)
      .expects(*)
      .onCall { bb: ByteBuffer =>
        bb.putInt(4)
        bb.put(Array[Byte](1, 2, 3, 4))
        8
      }
      .once()

    iterator.next() should contain theSameElementsInOrderAs Array[Byte](1, 2, 3, 4)

    (channel.read _)
      .expects(*)
      .onCall { bb: ByteBuffer =>
        bb.putInt(5)
        bb.put(Array[Byte](5, 6))
        6
      }
      .once()

    (channel.read _)
      .expects(*)
      .onCall { bb: ByteBuffer =>
        bb.put(Array[Byte](7, 8, 9))
        3
      }
      .once()

    iterator.next() should contain theSameElementsInOrderAs Array[Byte](5, 6, 7, 8, 9)
  }

  private def createFrame(data: Byte*): Array[Byte] = {
    val bb = ByteBuffer.allocate(data.length + 4)
    bb.putInt(data.length)
    bb.put(data.toArray)
    bb.array()
  }
}
