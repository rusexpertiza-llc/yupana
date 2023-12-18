/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.jdbc

import org.yupana.api.query.SimpleResult
import org.yupana.api.types.DataType
import org.yupana.api.utils.CollectionUtils
import org.yupana.jdbc.YupanaConnection.QueryResult
import org.yupana.jdbc.YupanaTcpClient.Handler
import org.yupana.jdbc.build.BuildInfo
import org.yupana.protocol._

import java.net.{ InetSocketAddress, StandardSocketOptions }
import java.nio.ByteBuffer
import java.nio.channels.{ AsynchronousSocketChannel, CompletionHandler }
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger
import java.util.{ Timer, TimerTask }
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }

class YupanaTcpClient(val host: String, val port: Int, batchSize: Int, user: String, password: String)(
    implicit ec: ExecutionContext
) extends AutoCloseable {

  private val logger = Logger.getLogger(classOf[YupanaTcpClient].getName)

  logger.info("New instance of YupanaTcpClient")

  private var channel: AsynchronousSocketChannel = _
  private var chanelReader: FramingChannelReader = _
  private val nextId: AtomicInteger = new AtomicInteger(0)
  private var iterators: Map[Int, ResultIterator] = Map.empty
  private val commandQueue: mutable.Queue[Handler[_]] = mutable.Queue.empty

  private val heartbeatTimer = new Timer()
  private val HEARTBEAT_INTERVAL = 30_000

  private var closed: Boolean = true

  private def ensureNotClosed(): Unit = {
    if (closed) throw new YupanaException("Connection is closed")
  }

  private def startHeartbeats(startTime: Long): Unit = {
    heartbeatTimer.schedule(
      new TimerTask {
        override def run(): Unit = sendHeartbeat(startTime)
      },
      HEARTBEAT_INTERVAL,
      HEARTBEAT_INTERVAL
    )
  }

  private def cancelHeartbeats(): Unit = {
    heartbeatTimer.cancel()
    heartbeatTimer.purge()
  }

  def prepareQuery(query: String, params: Map[Int, ParameterValue]): QueryResult = {
    val id = nextId.incrementAndGet()
    Await.result(execRequestQuery(id, SqlQuery(id, query, params)), Duration.Inf)
  }

  def batchQuery(query: String, params: Seq[Map[Int, ParameterValue]]): QueryResult = {
    val id = nextId.incrementAndGet()
    Await.result(execRequestQuery(id, BatchQuery(id, query, params)), Duration.Inf)
  }

  def connect(reqTime: Long): Unit = {
    logger.fine("Hello")

    if (channel == null || !channel.isOpen /* || !channel.isConnected*/ ) {
      logger.info(s"Connect to $host:$port")
      channel = AsynchronousSocketChannel.open()
      channel.setOption(StandardSocketOptions.SO_KEEPALIVE, java.lang.Boolean.TRUE)
      channel.connect(new InetSocketAddress(host, port)).get()

      chanelReader = new FramingChannelReader(channel, Frame.MAX_FRAME_SIZE + FramingChannelReader.PAYLOAD_OFFSET)
      closed = false
    }

    val cf = for {
      _ <- write(Hello(ProtocolVersion.value, BuildInfo.version, reqTime, Map.empty))
      _ <- waitHelloResponse(reqTime)
      cr <- waitFor(CredentialsRequest)
      _ <- sendCredentials(cr)
      _ <- waitFor(Authorized)
    } yield ()

    Await.result(cf, Duration.Inf)
    startHeartbeats(reqTime)
  }

  private def waitHelloResponse(reqTime: Long): Future[HelloResponse] = {
    waitFor(HelloResponse).flatMap { response =>
      if (response.protocolVersion != ProtocolVersion.value) {
        Future.failed(
          new YupanaException(
            error(
              s"Incompatible protocol versions: ${response.protocolVersion} on server and ${ProtocolVersion.value} in this driver"
            )
          )
        )
      } else if (response.reqTime != reqTime) {
        Future.failed(new YupanaException(error("got wrong hello response")))
      } else {
        Future.successful(response)
      }
    }
  }

  private def sendCredentials(cr: CredentialsRequest): Future[Unit] = {
    if (!cr.methods.contains(CredentialsRequest.METHOD_PLAIN)) {
      Future.failed(new YupanaException(error(s"All the auth methods ${cr.methods.mkString(", ")} are not supported")))
    } else {
      write(Credentials(CredentialsRequest.METHOD_PLAIN, user, password))
    }
  }

  private def sendHeartbeat(startTime: Long): Unit = {
    val time = (System.currentTimeMillis() - startTime) / 1000
    if (channel.isOpen) {
      Await.ready(write(Heartbeat(time.toInt)), Duration.Inf)
    }
  }

  private def runNext(): Future[Unit] = {
    commandQueue.headOption match {
      case Some(handler) =>
        for {
          _ <- write(handler.cmd)
          _ <- handler.execute()
          _ <- {
            commandQueue.synchronized {
              commandQueue.dequeue()
            }
            runNext()
          }
        } yield ()
      case None => Future.successful(())
    }
  }

  private def runCommand[T](cmd: Command[_], f: Promise[T] => Unit): Future[T] = {
    val p = Promise[T]()
    commandQueue.synchronized {
      commandQueue.enqueue(Handler(cmd, p, f))
    }
    runNext()

    p.future
  }

  private def readBatch(id: Int, read: Int): Future[Int] = {
    chanelReader.readFrame().flatMap { frame =>
      frame.frameType match {
        case Tags.RESULT_ROW =>
          val row = ResultRow.readFrame(frame)
          if (row.id == id) {
            iterators(id).addResult(row)
            if (read < batchSize) readBatch(id, read + 1) else Future.successful(read + 1)
          } else {
            Future.failed(new YupanaException(s"Unexpected row id ${row.id}"))
          }

        case Tags.RESULT_FOOTER =>
          iterators(id).setDone()
          Future.successful(read)

        case Tags.ERROR_MESSAGE =>
          val em = ErrorMessage.readFrame(frame)
          val msg = em.streamId.fold(s"A big problem here. ${em.message}") { i =>
            if (i == id) s"Unable to read batch. ${em.message}" else s"Unexpected row id $i in error. ${em.message}"
          }
          Future.failed(new YupanaException(msg))

        case x => Future.failed(new YupanaException(s"Unexpected response ${x.toChar} in Next handler"))
      }
    }
  }

  def acquireNext(id: Int): Unit = {
    assert(iterators.contains(id))
    val f = runCommand(
      Next(id, batchSize),
      (p: Promise[Unit]) => readBatch(id, 0).onComplete(x => p.complete(x.map(_ => ())))
    )
    Await.result(f, Duration.Inf)
  }

  def cancel(id: Int): Unit = {
    if (iterators.contains(id)) {
      val f = runCommand(
        Cancel(id),
        (p: Promise[Unit]) =>
          waitFor(Canceled).onComplete { cancelled =>
            cancelled.map { c =>
              assert(c.id == id)
              iterators.synchronized {
                iterators -= id
              }
            }
            p.complete(cancelled.map(_ => ()))
          }
      )

      Await.result(f, Duration.Inf)
    }
  }

  private def waitFor[T <: Message[T]](helper: MessageHelper[T])(
      implicit ec: ExecutionContext
  ): Future[T] = {
    chanelReader.readFrame().flatMap { frame =>
      frame.frameType match {
        case helper.tag => Future.successful(helper.readFrame[ByteBuffer](frame))
        case Tags.ERROR_MESSAGE =>
          val msg = ErrorMessage.readFrame(frame).message
          Future.failed(new YupanaException(error(s"Got error response on '${helper.tag.toChar}', '$msg'")))

        case x =>
          Future.failed(
            new YupanaException(error(s"Unexpected response '${x.toChar}' while waiting for '${helper.tag.toChar}'"))
          )
      }
    }
  }

  private def execRequestQuery(id: Int, command: Command[_]): Future[QueryResult] = {
    logger.fine(s"Exec request query $command")
    ensureNotClosed()
    runCommand(
      command,
      (p: Promise[ResultIterator]) =>
        waitFor(ResultHeader).onComplete { header =>
          p.complete(header.map(h => {
            iterators.synchronized {
              val r = new ResultIterator(h, this)
              iterators += id -> r
              r
            }
          }))
        }
    ).map(it => extractProtoResult(id, it))
  }

  private def write(request: Command[_]): Future[Unit] = {
    logger.fine(s"Writing command ${request.helper.tag.toChar}")
    val f = request.toFrame
    val bb = ByteBuffer.allocate(f.payload.length + 4 + 1)
    bb.put(f.frameType)
    bb.putInt(f.payload.length)
    bb.put(f.payload)
    bb.flip()

    JdbcUtils.wrapHandler[Unit](
      new CompletionHandler[Integer, Promise[Unit]] {
        override def completed(result: Integer, p: Promise[Unit]): Unit = p.success(())
        override def failed(exc: Throwable, p: Promise[Unit]): Unit = p.failure(exc)
      },
      (p, h) => channel.write(bb, p, h)
    )
  }

  private def error(e: String): String = {
    logger.warning(s"Got error message: $e")
    e
  }

  override def close(): Unit = {
    logger.fine("Close connection")
    closed = true
    cancelHeartbeats()
    Await.ready(write(Quit()), Duration.Inf)
    channel.close()
  }

  private def extractProtoResult(id: Int, res: ResultIterator): QueryResult = {
    val header = res.header
    val names = header.fields.map(_.name)
    val dataTypes = CollectionUtils.collectErrors(header.fields.map { resultField =>
      DataType.bySqlName(resultField.typeName).toRight(s"Unknown type ${resultField.typeName}")
    }) match {
      case Right(types) => types
      case Left(err)    => throw new IllegalArgumentException(s"Cannot read data: $err")
    }

    val values = res.map { row =>
      dataTypes
        .zip(row.values)
        .map {
          case (rt, bytes) =>
            if (bytes.isEmpty) {
              null
            } else {
              rt.storable.read(bytes)
            }
        }
        .toArray
    }

    QueryResult(id, SimpleResult(header.tableName, names, dataTypes, values))
  }
}

object YupanaTcpClient {
  private case class Handler[R](cmd: Command[_], promise: Promise[R], f: Promise[R] => Unit) {
    def execute(): Future[R] = {
      f(promise)
      promise.future
    }
  }
}
