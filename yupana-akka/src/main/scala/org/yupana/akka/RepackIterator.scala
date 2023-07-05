package org.yupana.akka

import akka.util.{ ByteString, ByteStringBuilder }
import com.typesafe.scalalogging.StrictLogging

class RepackIterator(it: Iterator[ByteString], packetSize: Int) extends Iterator[ByteString] with StrictLogging {

  private val buf = new ByteStringBuilder
  buf.sizeHint(packetSize)

  override def hasNext: Boolean = {
    it.hasNext || buf.nonEmpty
  }

  override def next(): ByteString = {
    while (buf.length < packetSize && it.hasNext) {
      buf.append(it.next())
    }
    val (r, b) = buf.result().splitAt(packetSize)
    buf.clear()
    buf.append(b)
    r
  }
}
