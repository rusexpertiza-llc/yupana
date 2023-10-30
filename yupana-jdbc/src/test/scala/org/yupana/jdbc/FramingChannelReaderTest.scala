package org.yupana.jdbc

import java.io.{ ByteArrayInputStream, IOException }
import java.nio.ByteBuffer
import java.nio.channels.{ Channels, ReadableByteChannel }
import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FramingChannelReaderTest extends AnyFlatSpec with Matchers with MockFactory with OptionValues {

  "FramingChannelReader" should "read frame from channel" in {
    val channel = Channels.newChannel(new ByteArrayInputStream(createFrame(1, 2, 3, 4, 5)))
    val reader = new FramingChannelReader(channel, 100)

    val bytes = reader.readFrame()
    bytes.value.payload should contain theSameElementsAs Array(1, 2, 3, 4, 5)
  }

  it should "throw an exception when channel is closed unexpectedly" in {
    val channel = mock[ReadableByteChannel]
    val iterator = new FramingChannelReader(channel, 100)

    (channel.read _).expects(*).onCall { bb: ByteBuffer =>
      bb.putInt(10)
      bb.put(Array[Byte](1, 2, 3, 4, 5))
      9
    }

    (channel.read _).expects(*).onCall((_: ByteBuffer) => -1)
    (channel.isOpen _).expects().onCall(() => true)
    (channel.close _).expects().once()

    an[IOException] should be thrownBy iterator.readFrame()
  }

  it should "return None when it cannot read frame size" in {
    val channel = Channels.newChannel(new ByteArrayInputStream(Array(1, 2, 3)))
    val reader = new FramingChannelReader(channel, 100)

    reader.readFrame() shouldBe None
  }

  it should "continue read several frames from channel" in {
    val channel = Channels.newChannel(
      new ByteArrayInputStream(
        createFrame(1, 2, 3) ++ createFrame(4, 5, 6, 7) ++ createFrame(8, 9)
      )
    )
    val reader = new FramingChannelReader(channel, 100)

    reader.readFrame().value.payload should contain theSameElementsAs Array(1, 2, 3)
    reader.readFrame().value.payload should contain theSameElementsAs Array(4, 5, 6, 7)
    reader.readFrame().value.payload should contain theSameElementsAs Array(8, 9)
  }

  it should "handle create a single chunk from parts" in {
    val channel = mock[ReadableByteChannel]

    val iterator = new FramingChannelReader(channel, 100)

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

    iterator.readFrame().value.payload should contain theSameElementsInOrderAs Array[Byte](1, 2, 3, 6, 7)
  }

  it should "handle buffer overflow" in {
    val channel = mock[ReadableByteChannel]

    val reader = new FramingChannelReader(channel, 14)

    (channel.isOpen _).expects().returning(true).anyNumberOfTimes()
    (channel.read _)
      .expects(*)
      .onCall { bb: ByteBuffer =>
        bb.putInt(4)
        bb.put(Array[Byte](1, 2, 3, 4))
        8
      }
      .once()

    reader.readFrame().value.payload should contain theSameElementsInOrderAs Array[Byte](1, 2, 3, 4)

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

    reader.readFrame().value.payload should contain theSameElementsInOrderAs Array[Byte](5, 6, 7, 8, 9)
  }

  private def createFrame(data: Byte*): Array[Byte] = {
    val bb = ByteBuffer.allocate(data.length + 4)
    bb.put(42.toByte)
    bb.putInt(data.length)
    bb.put(data.toArray)
    bb.array()
  }
}
