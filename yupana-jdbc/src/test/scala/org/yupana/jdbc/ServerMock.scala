package org.yupana.jdbc

import org.yupana.protocol.{ Message, Response }

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler }
import java.util.concurrent.TimeUnit
import scala.concurrent.{ Future, Promise }

class ServerMock {

  private val serverSock = AsynchronousServerSocketChannel.open().bind(null)

  def port: Int = serverSock.getLocalAddress.asInstanceOf[InetSocketAddress].getPort

  def readBytesSendResponseChunked(response: Response[_]): Future[Array[Byte]] = {
    readBytesSendResponses(Seq(response), ServerMock.pack)
  }

  def readBytesSendResponsesChunked(responses: Seq[Response[_]]): Future[Array[Byte]] = {
    readBytesSendResponses(responses, ServerMock.pack)
  }

  def readBytesSendResponse(raw: Array[Byte]): Future[Array[Byte]] = {
    readBytesSendResponses(Seq(raw), ByteBuffer.wrap)
  }
  def readBytesSendResponse(response: Response[_]): Future[Array[Byte]] = {
    readBytesSendResponses(Seq(response), ServerMock.pack)
  }

  def readBytesSendResponses[T](responses: Seq[T], pack: T => ByteBuffer): Future[Array[Byte]] = {
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
          v.close()
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
  def pack(data: Message[_]): ByteBuffer = {
    val frame = data.toFrame[ByteBuffer]
    val resp = ByteBuffer.allocate(frame.payload.length + 4 + 1)
    resp.put(frame.frameType)
    resp.putInt(frame.payload.length)
    resp.put(frame.payload)
    resp.flip()
    resp
  }
}
