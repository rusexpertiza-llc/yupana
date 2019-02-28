package org.yupana.jdbc

import java.nio.channels.ReadableByteChannel
import java.nio.{ByteBuffer, ByteOrder}

class FramingChannelIterator(
  channel: ReadableByteChannel,
  frameSize: Int
) extends Iterator[Array[Byte]]{

  private val buffer = createBuffer

  private val intArray = Array.ofDim[Byte](4)
  private val intBuffer = ByteBuffer.wrap(intArray)

  private def createBuffer = {
    val buf = ByteBuffer.allocate(frameSize).order(ByteOrder.BIG_ENDIAN)
    buf.flip()
    buf
  }

  private var atEnd = false

  override def hasNext: Boolean = {
    if (channel.isOpen && buffer.remaining() == 0) fetch(1)
    !atEnd && channel.isOpen
  }

  override def next(): Array[Byte] = {
    if (!hasNext) throw new IllegalStateException("Next is called on empty iterator")
    read(intArray)
    intBuffer.clear()
    val chunkSize = intBuffer.getInt()

    val result = Array.ofDim[Byte](chunkSize)
    read(result)
    result
  }

  private def read(dst: Array[Byte], offset: Int = 0): Int = {
    val bytesRequired = dst.length - offset
    var totalRead = fetch(bytesRequired)
    buffer.get(dst, offset, math.min(totalRead, bytesRequired))

    if (totalRead < bytesRequired && !atEnd) {
      buffer.clear()
      buffer.flip()
      totalRead += read(dst, totalRead)
    }

    if (totalRead < bytesRequired) throw new IllegalStateException(s"$bytesRequired bytes requested but got only $totalRead")

    totalRead
  }

  private def fetch(size: Int): Int = {
    var lastRead = 0
    var totalRead = buffer.remaining()
    buffer.mark()
    buffer.position(buffer.limit())
    buffer.limit(buffer.capacity())
    while (totalRead < size && buffer.remaining() > 0 && lastRead != -1) {
      lastRead = channel.read(buffer)
      if (lastRead != -1) {
        totalRead += lastRead
      }
    }

    buffer.limit(buffer.position())
    buffer.reset
    atEnd = lastRead == -1
    totalRead
  }

 }
