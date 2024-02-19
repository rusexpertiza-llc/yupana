package org.yupana.jdbc

import java.nio.ByteBuffer
import java.nio.channels.{ AsynchronousByteChannel, CompletionHandler }
import java.util.concurrent.{ CompletableFuture, Future }

class SimpleAsyncByteChannel(data: Array[Byte]) extends AsynchronousByteChannel {

  private var closed = false
  private var done = false
  override def read[A](dst: ByteBuffer, attachment: A, handler: CompletionHandler[Integer, _ >: A]): Unit = {
    if (!done) {
      dst.put(data)
      done = true
      handler.completed(data.length, attachment)
    } else {
      handler.completed(-1, attachment)
    }
  }

  override def read(dst: ByteBuffer): Future[Integer] = {
    if (!done) {
      dst.put(data)
      done = true
      CompletableFuture.completedFuture(data.length)
    } else {
      CompletableFuture.completedFuture(-1)
    }
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
