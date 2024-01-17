package org.yupana.jdbc

import org.yupana.protocol.{ Frame, Message }

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler }
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ Future, Promise }

class ServerMock {

  private val serverSock = AsynchronousServerSocketChannel.open().bind(null)

  def port: Int = serverSock.getLocalAddress.asInstanceOf[InetSocketAddress].getPort

  private var sockets: Map[Int, AsynchronousSocketChannel] = Map.empty
  private val nextId: AtomicInteger = new AtomicInteger(0)

  def connect: Future[Int] = {
    val res = Promise[Int]()
    serverSock.accept(
      nextId.incrementAndGet(),
      new CompletionHandler[AsynchronousSocketChannel, Int] {
        override def completed(result: AsynchronousSocketChannel, attachment: Int): Unit = {
          sockets += attachment -> result
          res.success(attachment)
        }

        override def failed(exc: Throwable, attachment: Int): Unit = res.failure(exc)
      }
    )

    res.future
  }

  def close(id: Int): Unit = {
    val s = sockets(id)
    sockets -= id
    s.close()
  }

  def readAndSendResponses[T](id: Int, parse: Frame => T, respond: T => Seq[Message[_]]): Future[T] = {
    readBytesSendResponsesAndPack(id, parse, respond, ServerMock.pack)
  }

  def readAnySendRaw[T](id: Int, parse: Frame => T, respond: T => Seq[Array[Byte]]): Future[T] = {
    readBytesSendResponsesAndPack(id, parse, respond, ByteBuffer.wrap)
  }

  private def readBytesSendResponsesAndPack[R, X](
      id: Int,
      parse: Frame => X,
      respond: X => Seq[R],
      pack: R => ByteBuffer
  ): Future[X] = {
    val p = Promise[X]()

    val s = sockets(id)
    val bb = ByteBuffer.allocate(16 * 1024)

    s.read(
      bb,
      null,
      new CompletionHandler[Integer, AnyRef] {
        override def completed(result: Integer, attachment: AnyRef): Unit = {
          try {
            bb.flip()
            val frameType = bb.get()
            val reqSize = bb.getInt()
            val bytes = new Array[Byte](reqSize)
            bb.get(bytes)
            val frame = Frame(frameType, bytes)
            val cmd = parse(frame)
            val responses = respond(cmd)
            responses foreach { response =>
              val resp = pack(response)
              s.write(resp).get(1, TimeUnit.SECONDS)
            }
            p.success(cmd)
          } catch {
            case e: Throwable => p.failure(e)
          }
        }

        override def failed(exc: Throwable, attachment: AnyRef): Unit = p.failure(exc)
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
