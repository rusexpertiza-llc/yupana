package org.yupana.jdbc

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler }
import java.util.concurrent.TimeUnit

import scala.concurrent.{ Future, Promise }

class ServerMock {

  private val serverSock = AsynchronousServerSocketChannel.open().bind(null)

  def port: Int = serverSock.getLocalAddress.asInstanceOf[InetSocketAddress].getPort

  def readBytesSendResponseChunked(response: Array[Byte]): Future[Array[Byte]] = {
    readBytesSendResponses(Seq(response), ServerMock.chunked)
  }

  def readBytesSendResponsesChunked(responses: Seq[Array[Byte]]): Future[Array[Byte]] = {
    readBytesSendResponses(responses, ServerMock.chunked)
  }

  def readBytesSendResponse(response: Array[Byte]): Future[Array[Byte]] = {
    readBytesSendResponses(Seq(response), ServerMock.raw)
  }

  def readBytesSendResponses(responses: Seq[Array[Byte]], pack: Array[Byte] => ByteBuffer): Future[Array[Byte]] = {
    val p = Promise[Array[Byte]]()

    serverSock.accept(
      null,
      new CompletionHandler[AsynchronousSocketChannel, AnyRef] {
        override def completed(v: AsynchronousSocketChannel, a: AnyRef): Unit = {
          val bb = ByteBuffer.allocate(16 * 1024)
          v.read(bb).get(1, TimeUnit.SECONDS)
          bb.flip()
          val reqSize = bb.getInt()
          val bytes = new Array[Byte](reqSize)
          bb.get(bytes)
          responses foreach { response =>
            val resp = pack(response)
            v.write(resp).get(1, TimeUnit.SECONDS)
          }
          p.success(bytes)
        }

        override def failed(throwable: Throwable, a: AnyRef): Unit = p.failure(throwable)
      }
    )

    p.future
  }

  def closeOnReceive(): Future[Unit] = {
    val p = Promise[Unit]()
    serverSock.accept(
      null,
      new CompletionHandler[AsynchronousSocketChannel, AnyRef] {
        override def completed(v: AsynchronousSocketChannel, a: AnyRef): Unit = {
          v.close()
          p.success(())
        }

        override def failed(throwable: Throwable, a: AnyRef): Unit = p.failure(throwable)
      }
    )

    p.future
  }

  def close(): Unit = {
    serverSock.close()
  }
}

object ServerMock {
  def chunked(data: Array[Byte]): ByteBuffer = {
    val resp = ByteBuffer.allocate(data.length + 4)
    resp.putInt(data.length)
    resp.put(data)
    resp.flip()
    resp
  }

  def raw(data: Array[Byte]): ByteBuffer = ByteBuffer.wrap(data)

}
