package org.yupana.jdbc

import org.yupana.jdbc.ServerMock.Step
import org.yupana.protocol.{ Frame, Message }

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler }
import java.util.concurrent.TimeUnit
import scala.concurrent.{ ExecutionContext, Future, Promise }

class ServerMock {

  private val serverSock = AsynchronousServerSocketChannel.open().bind(null)

  def port: Int = serverSock.getLocalAddress.asInstanceOf[InetSocketAddress].getPort

  def readAndSendResponses[T](step: Step.Aux[Message[_], T])(implicit ec: ExecutionContext): Future[T] = {
    readBytesSendResponsesAndPack(Seq(step), ServerMock.pack).map(_.head.asInstanceOf[T])
  }

  def read2AndSendResponses[T, U](step1: Step.Aux[Message[_], T], step2: Step.Aux[Message[_], U])(
      implicit ec: ExecutionContext
  ): Future[(T, U)] = {
    readBytesSendResponsesAndPack(Seq(step1, step2), ServerMock.pack).map(x =>
      (x(0).asInstanceOf[T], x(1).asInstanceOf[U])
    )
  }

  def readAnySendRaw[T](step: Step.Aux[Array[Byte], T])(implicit ec: ExecutionContext): Future[Array[Byte]] = {
    readBytesSendResponsesAndPack(Seq(step), ByteBuffer.wrap).map(_.head.asInstanceOf[Array[Byte]])
  }

  private def readBytesSendResponsesAndPack[R](
      scenario: Seq[Step[R]],
      pack: R => ByteBuffer
  ): Future[Seq[Any]] = {
    val p = Promise[Seq[Any]]()

    serverSock.accept(
      null,
      new CompletionHandler[AsynchronousSocketChannel, AnyRef] {
        override def completed(v: AsynchronousSocketChannel, a: AnyRef): Unit = {
          val results = scenario.map { step =>
            val bb = ByteBuffer.allocate(16 * 1024)
            v.read(bb).get(1, TimeUnit.SECONDS)
            bb.flip()
            val frameType = bb.get()
            val reqSize = bb.getInt()
            val bytes = new Array[Byte](reqSize)
            bb.get(bytes)
            val frame = Frame(frameType, bytes)
            val cmd = step.parse(frame)
            val responses = step.respond(cmd)
            responses foreach { response =>
              val resp = pack(response)
              v.write(resp).get(1, TimeUnit.SECONDS)
            }
            cmd
          }
          p.success(results)
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

  sealed trait Step[U] {
    type T
    val parse: Frame => T
    val respond: T => Seq[U]
  }

  object Step {

    type Aux[R, I] = Step[R] { type T = I }
    def apply[R, I](p: Frame => I)(r: I => Seq[R]): Step.Aux[R, I] = new Step[R] {
      override type T = I
      override val parse: Frame => I = p
      override val respond: I => Seq[R] = r
    }
  }

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
