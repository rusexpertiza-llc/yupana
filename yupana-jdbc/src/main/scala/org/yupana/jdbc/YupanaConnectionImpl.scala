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
import org.yupana.api.types.{ ByteReaderWriter, DataType }
import org.yupana.api.utils.CollectionUtils
import org.yupana.jdbc.YupanaConnection.QueryResult
import org.yupana.jdbc.build.BuildInfo
import org.yupana.protocol._
import org.yupana.serialization.ByteBufferEvalReaderWriter

import java.io.IOException
import java.net.{ InetSocketAddress, StandardSocketOptions }
import java.nio.ByteBuffer
import java.nio.channels.{ AsynchronousSocketChannel, CompletionHandler }
import java.sql.SQLException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger
import java.util.{ Properties, Timer, TimerTask }
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success }

class YupanaConnectionImpl(override val url: String, properties: Properties, executionContext: ExecutionContext)
    extends YupanaConnection {

  implicit private val ec: ExecutionContext = executionContext

  private val logger = Logger.getLogger(classOf[YupanaConnectionImpl].getName)

  private var channel: AsynchronousSocketChannel = _
  private var chanelReader: FramingChannelReader = _
  private val nextId: AtomicInteger = new AtomicInteger(0)
  private var iterators: Map[Int, ResultIterator] = Map.empty
  private val commandQueue: mutable.Queue[Handler[_]] = mutable.Queue.empty

  private val heartbeatTimer = new Timer(true)
  private val HEARTBEAT_INTERVAL = 30_000

  private var closed: Boolean = true

  implicit val readerWriter: ByteReaderWriter[ByteBuffer] = ByteBufferEvalReaderWriter

  private val host = properties.getProperty("yupana.host")
  private val port = properties.getProperty("yupana.port").toInt
  private val batchSize = Option(properties.getProperty("yupana.batchSize")).map(_.toInt).getOrElse(100)
  private val user = Option(properties.getProperty("user")).filter(_.nonEmpty)
  private val password = Option(properties.getProperty("password")).filter(_.nonEmpty)

  connect(System.currentTimeMillis())

  override def runQuery(query: String, params: Map[Int, ParameterValue]): QueryResult = {
    val id = nextId.incrementAndGet()
    wrapError(execRequestQuery(id, SqlQuery(id, query, params)))
  }

  override def runBatchQuery(query: String, params: Seq[Map[Int, ParameterValue]]): QueryResult = {
    val id = nextId.incrementAndGet()
    wrapError(execRequestQuery(id, BatchQuery(id, query, params)))
  }

  override def cancelStream(streamId: Int): Unit = {
    if (iterators.contains(streamId)) {
      val f = runCommand(
        Cancel(streamId),
        (p: Promise[Unit]) =>
          waitFor(Cancelled).onComplete { cancelled =>
            cancelled.map { c =>
              assert(c.id == streamId)
              iterators.synchronized {
                iterators -= streamId
              }
            }
            p.complete(cancelled.map(_ => ()))
          }
      )

      wrapError(f)
    }
  }

  @throws[SQLException]
  override def isClosed: Boolean = closed

  private def wrapError[T](r: => Future[T]): T = {
    try {
      Await.result(r, Duration.Inf)
    } catch {
      case io: IOException =>
        channel.close()
        closed = true
        cancelHeartbeats()
        throw new SQLException("Connection problem, closing", io)

      case e: SQLException => throw e
      case x: Throwable    => throw new SQLException(x)
    }
  }

  private def ensureNotClosed(): Unit = {
    closed &= !channel.isOpen
    if (closed) {
      cancelHeartbeats()
      throw new YupanaException("Connection is closed")
    }
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

  private def connect(reqTime: Long): Unit = {
    logger.fine("Hello")

    if (channel == null || !channel.isOpen /* || !channel.isConnected*/ ) {
      logger.info(s"Connect to $host:$port")
      channel = AsynchronousSocketChannel.open()
      channel.setOption(StandardSocketOptions.SO_KEEPALIVE, java.lang.Boolean.FALSE)
      channel.connect(new InetSocketAddress(host, port)).get()

      chanelReader = new FramingChannelReader(channel, Frame.MAX_FRAME_SIZE + FramingChannelReader.PAYLOAD_OFFSET)
    }

    val cf = for {
      _ <- write(Hello(ProtocolVersion.value, BuildInfo.version, reqTime, Map.empty))
      _ <- waitHelloResponse(reqTime)
      cr <- waitFor(CredentialsRequest)
      _ <- sendCredentials(cr)
      _ <- waitFor(Authorized)
    } yield {
      closed = false
    }

    Await.result(cf, Duration.Inf)
    startHeartbeats(reqTime)
  }

  private def waitHelloResponse(reqTime: Long): Future[HelloResponse] = {
    waitFor(HelloResponse).flatMap { response =>
      if (response.protocolVersion != ProtocolVersion.value) {
        val msg =
          s"Incompatible protocol versions: ${response.protocolVersion} on server and ${ProtocolVersion.value} in this driver"
        logger.severe(msg)
        Future.failed(new YupanaException(msg))
      } else if (response.reqTime != reqTime) {
        logger.severe(s"Request time $reqTime != response time ${response.reqTime}")
        Future.failed(new YupanaException("Got wrong hello response"))
      } else {
        Future.successful(response)
      }
    }
  }

  private def sendCredentials(cr: CredentialsRequest): Future[Unit] = {
    if (!cr.methods.contains(CredentialsRequest.METHOD_PLAIN)) {
      val msg = s"All the auth methods ${cr.methods.mkString(", ")} are not supported"
      logger.warning(msg)
      Future.failed(new YupanaException(msg))
    } else {
      write(Credentials(CredentialsRequest.METHOD_PLAIN, user, password))
    }
  }

  private def sendHeartbeat(startTime: Long): Unit = {
    val time = (System.currentTimeMillis() - startTime) / 1000
    if (channel.isOpen) {
      Await.result(write(Heartbeat(time.toInt)), Duration.Inf)
    } else {
      cancelHeartbeats()
    }
  }

  private def runNext(): Future[Unit] = {
    val handler: Option[Handler[_]] = commandQueue.synchronized {
      if (commandQueue.nonEmpty) Some(commandQueue.dequeue()) else None
    }

    handler match {
      case Some(handler) =>
        for {
          _ <- handler.execute()
          _ <- runNext()
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
        case Tags.RESULT_ROW.value =>
          val row = ResultRow.readFrame(frame)
          if (row.id == id) {
            val newRead = read + 1
            iterators(id).addResult(row)
            if (newRead < batchSize) readBatch(id, newRead) else Future.successful(newRead)
          } else {
            Future.failed(new YupanaException(s"Unexpected row id ${row.id}"))
          }

        case Tags.RESULT_FOOTER.value =>
          iterators(id).setDone()
          iterators.synchronized {
            iterators -= id
          }
          Future.successful(read)

        case Tags.ERROR_MESSAGE.value =>
          val em = ErrorMessage.readFrame(frame)
          val ex = new YupanaException(em.message)

          em.streamId match {
            case Some(sId) =>
              logger.info(s"Got error message $em")
              failIterator(sId, ex)
              if (sId == id) {
                Future.failed(ex)
              } else {
                logger.severe(s"Unexpected error message '${em.message}'")
                readBatch(id, read)
              }

            case None =>
              logger.warning(s"Got global error message $em")
              iterators.synchronized {
                iterators.foreach(_._2.setFailed(ex))
                iterators = Map.empty
              }
              Future.failed(ex)
          }

          Future.failed(ex)

        case x => Future.failed(new YupanaException(s"Unexpected response ${x.toChar} in Next handler"))
      }
    }
  }

  private def failIterator(id: Int, ex: Throwable): Unit = {
    iterators.synchronized {
      iterators.get(id).foreach(_.setFailed(ex))
      iterators -= id
    }
  }

  private def acquireNext(id: Int): Unit = {
    logger.fine(s"Acquire next $id")
    assert(iterators.contains(id))
    val f = runCommand(
      NextBatch(id, batchSize),
      (p: Promise[Unit]) => readBatch(id, 0).onComplete(x => p.complete(x.map(_ => ())))
    )
    wrapError(f)
  }

  private def waitFor[T <: Message[T]](helper: MessageHelper[T])(
      implicit ec: ExecutionContext
  ): Future[T] = {
    chanelReader.readFrame().flatMap { frame =>
      frame.frameType match {
        case x if x == helper.tag.value => Future.successful(helper.readFrame[ByteBuffer](frame))
        case Tags.ERROR_MESSAGE.value =>
          val msg = ErrorMessage.readFrame(frame).message
          logger.warning(s"Got error response on '${helper.tag.value.toChar}', '$msg'")
          Future.failed(new YupanaException(msg))

        case x =>
          val error = s"Unexpected response '${x.toChar}' while waiting for '${helper.tag.value.toChar}'"
          logger.severe(error)
          Future.failed(new YupanaException(error))
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
              val r = new ResultIterator(h, () => acquireNext(id))
              iterators += id -> r
              r
            }
          }))
        }
    ).map(it => extractProtoResult(id, it))
  }

  private def write(request: Command[_]): Future[Unit] = {
    logger.fine(s"Writing command ${request.helper.tag.value.toChar}")
    val f = request.toFrame[ByteBuffer](ByteBuffer.allocate(Frame.MAX_FRAME_SIZE))
    val bb = ByteBuffer.allocate(f.payload.position() + 4 + 1)
    bb.put(f.frameType)
    bb.putInt(f.payload.position())
    f.payload.flip()
    bb.put(f.payload)
    bb.flip()

    JdbcUtils.wrapHandler[Unit](
      new CompletionHandler[Integer, Promise[Unit]] {
        override def completed(result: Integer, p: Promise[Unit]): Unit = p.success(())
        override def failed(exc: Throwable, p: Promise[Unit]): Unit = p.failure(exc)
      },
      (p, h) => channel.write(bb, 10, TimeUnit.SECONDS, p, h)
    )
  }

  override def close(): Unit = {
    logger.info("Close connection")
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
              val b = ByteBuffer.wrap(bytes)
              rt.storable.read(b)
            }
        }
        .toArray
    }

    QueryResult(id, SimpleResult(header.tableName, names, dataTypes, values))
  }

  private case class Handler[R](cmd: Command[_], promise: Promise[R], f: Promise[R] => Unit) {
    def execute(): Future[R] = {
      write(cmd) onComplete {
        case Success(_)         => f(promise)
        case Failure(exception) => promise.failure(exception)
      }

      promise.future
    }
  }
}
