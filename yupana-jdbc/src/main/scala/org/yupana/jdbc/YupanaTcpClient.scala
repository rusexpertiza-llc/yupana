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
import java.util.logging.Logger
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }

class YupanaTcpClient(val host: String, val port: Int, batchSize: Int, user: String, password: String)
    extends AutoCloseable {

  private val logger = Logger.getLogger(classOf[YupanaTcpClient].getName)

  logger.info("New instance of YupanaTcpClient")

  private var channel: AsynchronousSocketChannel = _
  private var chanelReader: FramingChannelReader = _
  private val nextId: AtomicInteger = new AtomicInteger(0)
  private var iterators: Map[Int, ResultIterator] = Map.empty
//  private val frames: mutable.Queue[Frame] = mutable.Queue.empty

  private var closed = true

  private def ensureNotClosed(): Unit = {
    if (closed) throw new YupanaException("Connection is closed")
  }

  def prepareQuery(query: String, params: Map[Int, ParameterValue])(implicit ec: ExecutionContext): Result = {
    val id = nextId.incrementAndGet()
    Await.result(execRequestQuery(id, PrepareQuery(id, query, params)), Duration.Inf)
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
    // scheduleHeartbeatTimer()

    Await.result(cf, Duration.Inf)
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

  def acquireNext(id: Int): Unit = {
    assert(iterators.contains(id))

    write(Next(id, batchSize))
    var errorMessage: String = null
    var footer: ResultFooter = null

    var count = 0

    do {
      // TODO: Rewrite non blocking!!!
      val frame = chanelReader.awaitAndReadFrame()

      frame.frameType match {
        case Tags.RESULT_ROW =>
          val res = ResultRow.readFrame(frame)
          iterators(res.id).addResult(res)
          if (res.id == id) count += 1

        case Tags.HEARTBEAT =>
          heartbeat(Heartbeat.readFrame(frame).time)

        case Tags.ERROR_MESSAGE =>
          errorMessage = error(ErrorMessage.readFrame(frame).message)

        case Tags.RESULT_FOOTER =>
          val ftr = ResultFooter.readFrame(frame)
          logger.fine(s"Got footer $ftr")
          //              scheduleHeartbeatTimer()
          iterators(ftr.id).setDone()
          footer = ftr

        case x =>
          logger.severe(s"Unexpected message type '${x.toChar}'")
          throw new IllegalStateException(s"Unexpected message type '${x.toChar}'")
      }
    } while (count < batchSize && footer == null && errorMessage == null)

    if (footer != null || errorMessage != null) {
      if (errorMessage != null) {
        throw new YupanaException(errorMessage)
      }
    }
  }

  private def waitFor[T <: Message[T]](helper: MessageHelper[T])(
      implicit ec: ExecutionContext
  ): Future[T] = {
    println(s"CALL WAIT FOR ${helper.tag.toChar}")
    chanelReader.readFrame().flatMap { frame =>
      println(s"GOT ${frame.frameType.toChar}")
      frame.frameType match {
        case helper.tag => Future.successful(helper.readFrame[ByteBuffer](frame))
        case Tags.HEARTBEAT =>
          val hb = Heartbeat.readFrame(frame)
          heartbeat(hb.time)
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

  private def execRequestQuery(id: Int, command: Command[_])(implicit ec: ExecutionContext): Future[Result] = {
    logger.fine(s"Exec request query $command")
    ensureNotClosed()
    //    cancelHeartbeatTimer()
    for {
      _ <- write(command)
      header <- waitFor(ResultHeader)
    } yield {
      val r = new ResultIterator(id, this)
      iterators += id -> r
      extractProtoResult(header, r)
    }
  }

  private def write(request: Command[_]): Future[Unit] = {
    val f = request.toFrame
    val bb = ByteBuffer.allocate(f.payload.length + 4 + 1)
    bb.put(f.frameType)
    bb.putInt(f.payload.length)
    bb.put(f.payload)
    bb.flip()

    val p = Promise[Unit]()

    channel.write(
      bb,
      null,
      new CompletionHandler[Integer, Any] {
        override def completed(result: Integer, attachment: Any): Unit = p.success(())
        override def failed(exc: Throwable, attachment: Any): Unit = p.failure(exc)
      }
    )

    p.future
  }

//  private def tryToReadHeartbeat(): Unit = {
//    if (channel.isOpen && channel.isConnected) {
//      val fr =
//        try {
//          chanelReader.readFrame()
//        } catch {
//          case _: IOException => None
//        }
//
//      fr.foreach { frame =>
//        frame.frameType match {
//          case Tags.HEARTBEAT => heartbeat(Heartbeat.readFrame(frame).time)
//          case x              => throw new IOException(s"Unexpected response '${x.toChar}'")
//        }
//      }
//    }
//  }

  private def error(e: String): String = {
    logger.warning(s"Got error message: $e")
    e
  }

  private def heartbeat(time: Int): Unit = {
    val msg = s"Heartbeat($time)"
    logger.fine(msg)
  }

  override def close(): Unit = {
    logger.fine("Close connection")
//    cancelHeartbeatTimer()
    closed = true
    channel.close()
  }

  private def extractProtoResult(header: ResultHeader, res: Iterator[ResultRow]): Result = {
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
