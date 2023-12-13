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

import org.yupana.api.query.{ Result, SimpleResult }
import org.yupana.api.types.DataType
import org.yupana.api.utils.CollectionUtils
import org.yupana.jdbc.build.BuildInfo
import org.yupana.protocol._

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ AsynchronousSocketChannel, CompletionHandler }
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.{ Level, Logger }
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.util.{ Failure, Success, Try }

class YupanaTcpClient(val host: String, val port: Int, batchSize: Int, user: String, password: String)
    extends AutoCloseable {

  private val logger = Logger.getLogger(classOf[YupanaTcpClient].getName)

  logger.info("New instance of YupanaTcpClient")

  private var channel: AsynchronousSocketChannel = _
  private var chanelReader: FramingChannelReader = _
  private val nextId: AtomicInteger = new AtomicInteger(0)
  private var iterators: Map[Int, ResultIterator] = Map.empty
  private var promises: Map[Int, Promise[ResultIterator]] = Map.empty
  private var countdowns: Map[Int, Countdown] = Map.empty
  private var closed: Boolean = true

  private def ensureNotClosed(): Unit = {
    if (closed) throw new YupanaException("Connection is closed")
  }

  def prepareQuery(query: String, params: Map[Int, ParameterValue])(implicit ec: ExecutionContext): Result = {
    val id = nextId.incrementAndGet()
    Await.result(execRequestQuery(id, SqlQuery(id, query, params)), Duration.Inf)
  }

  def batchQuery(query: String, params: Seq[Map[Int, ParameterValue]])(implicit ec: ExecutionContext): Result = {
    val id = nextId.incrementAndGet()
    Await.result(execRequestQuery(id, BatchQuery(id, query, params)), Duration.Inf)
  }

  def connect(reqTime: Long)(implicit ec: ExecutionContext): Unit = {
    logger.fine("Hello")

    if (channel == null || !channel.isOpen /* || !channel.isConnected*/ ) {
      logger.info(s"Connect to $host:$port")
      channel = AsynchronousSocketChannel.open()
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
    chanelReader.readFrame().onComplete(frameHandler)
  }

  private def frameHandler(implicit ec: ExecutionContext): Try[Frame] => Unit = {
    case Success(frame) =>
      frame.frameType match {
        case Tags.HEARTBEAT => heartbeat(Heartbeat.readFrame(frame))
        case Tags.ERROR_MESSAGE =>
          val em = ErrorMessage.readFrame(frame)
          em.streamId match {
            case Some(id) =>
              if (iterators.contains(id)) {
                iterators.synchronized {
                  iterators(id).setFailed(new YupanaException(em.message))
                  iterators -= id
                }
              }
              if (promises.contains(id)) {
                promises.synchronized {
                  promises(id).failure(new YupanaException(em.message))
                  promises -= id
                }
              }
              if (countdowns.contains(id)) {
                countdowns.synchronized {
                  countdowns(id).failure(new YupanaException(em.message))
                  countdowns -= id
                }
              }

            case None =>
              promises.foreach(_._2.failure(new YupanaException(em.message)))
              iterators.foreach(_._2.setFailed(new YupanaException(em.message)))
              promises = Map.empty
              iterators = Map.empty
          }

        case Tags.RESULT_HEADER =>
          val h = ResultHeader.readFrame(frame)
          val p = promises(h.id)
          val resultIterator = new ResultIterator(h, this)
          promises.synchronized {
            promises -= h.id
          }
          iterators.synchronized {
            iterators += h.id -> resultIterator
          }
          p.success(resultIterator)

        case Tags.RESULT_ROW =>
          val res = ResultRow.readFrame(frame)
          iterators(res.id).addResult(res)
          if (countdowns(res.id).release() == 0) {
            countdowns.synchronized {
              countdowns -= res.id
            }
          }

        case Tags.RESULT_FOOTER =>
          val ftr = ResultFooter.readFrame(frame)
          logger.fine(s"Got footer $ftr")
          iterators.synchronized {
            iterators(ftr.id).setDone()
            iterators -= ftr.id
          }
          countdowns.synchronized {
            countdowns(ftr.id).cancel()
            countdowns -= ftr.id
          }

        case t =>
          logger.warning(s"Got unexpected frame ${t.toChar}")
          throw new YupanaException(s"Unexpected frame ${t.toChar}")
      }
      chanelReader.readFrame().onComplete(frameHandler)

    case Failure(e) =>
      promises.synchronized {
        promises.foreach(_._2.failure(e))
        promises = Map.empty
      }
      iterators.synchronized {
        iterators.foreach(_._2.setFailed(e))
        iterators = Map.empty
      }
      countdowns.synchronized {
        countdowns.foreach(_._2.failure(e))
        countdowns = Map.empty
      }

      logger.log(Level.SEVERE, "Unable to read frame", e)
      close()
  }

  private def waitHelloResponse(reqTime: Long)(implicit ec: ExecutionContext): Future[HelloResponse] = {
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

  def acquireNext(id: Int)(implicit ec: ExecutionContext): Future[Unit] = {
    assert(iterators.contains(id))
    val countdown = new Countdown(batchSize)
    countdowns += id -> countdown
    for {
      _ <- write(Next(id, batchSize))
      _ <- countdown.future
    } yield ()
  }

  private def waitFor[T <: Message[T]](helper: MessageHelper[T])(
      implicit ec: ExecutionContext
  ): Future[T] = {
    chanelReader.readFrame().flatMap { frame =>
      frame.frameType match {
        case helper.tag => Future.successful(helper.readFrame[ByteBuffer](frame))
        case Tags.HEARTBEAT =>
          val hb = Heartbeat.readFrame(frame)
          heartbeat(hb)
          waitFor(helper)
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

  private def waitForHeader(id: Int): Future[ResultIterator] = {
    val p = Promise[ResultIterator]()
    promises += id -> p
    p.future
  }

  private def execRequestQuery(id: Int, command: Command[_])(implicit ec: ExecutionContext): Future[Result] = {
    logger.fine(s"Exec request query $command")
    ensureNotClosed()
    for {
      _ <- write(command)
      r <- waitForHeader(id)
    } yield {
      extractProtoResult(r)
    }
  }

  private def write(request: Command[_]): Future[Unit] = {
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

  private def heartbeat(heartbeat: Heartbeat): Unit = {
    val msg = s"Heartbeat(${heartbeat.time})"
    logger.fine(msg)
  }

  override def close(): Unit = {
    logger.fine("Close connection")
    closed = true
    channel.close()
  }

  private def extractProtoResult(res: ResultIterator): Result = {
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

    SimpleResult(header.tableName, names, dataTypes, values)
  }
}
