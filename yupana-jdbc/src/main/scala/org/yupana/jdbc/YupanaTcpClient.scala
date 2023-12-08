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
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger
import scala.annotation.tailrec

class YupanaTcpClient(val host: String, val port: Int, batchSize: Int, user: String, password: String)
    extends AutoCloseable {

  private val logger = Logger.getLogger(classOf[YupanaTcpClient].getName)

  logger.info("New instance of YupanaTcpClient")

  private var channel: AsynchronousSocketChannel = _
  private var chanelReader: FramingChannelReader = _
  private val nextId: AtomicInteger = new AtomicInteger(0)
  private var iterators: Map[Int, ResultIterator] = Map.empty

//  private var heartbeatTimer: java.util.Timer = _
//  private var heartbeatTimerScheduled = false

  private var closed = true

//  private def scheduleHeartbeatTimer(): Unit = {
//    heartbeatTimer = new Timer()
//    val heartbeatTask = new TimerTask {
//      override def run(): Unit = tryToReadHeartbeat()
//    }
//    heartbeatTimerScheduled = true
//    heartbeatTimer.schedule(heartbeatTask, HEARTBEAT_PERIOD, HEARTBEAT_PERIOD)
//  }
//  private def cancelHeartbeatTimer(): Unit = {
//    if (heartbeatTimerScheduled) {
//      heartbeatTimerScheduled = false
//      heartbeatTimer.cancel()
//      heartbeatTimer.purge()
//    }
//  }

  private def ensureNotClosed(): Unit = {
    if (closed) throw new YupanaException("Connection is closed")
  }

  def prepareQuery(query: String, params: Map[Int, ParameterValue]): Result = {
    val id = nextId.incrementAndGet()
    execRequestQuery(id, PrepareQuery(id, query, params))
  }

  def batchQuery(query: String, params: Seq[Map[Int, ParameterValue]]): Result = {
    val id = nextId.incrementAndGet()
    execRequestQuery(id, BatchQuery(id, query, params))
  }

  def connect(reqTime: Long): Unit = {
    logger.fine("Hello")

    if (channel == null || !channel.isOpen /* || !channel.isConnected*/ ) {
      logger.info(s"Connect to $host:$port")
      channel = AsynchronousSocketChannel.open()
      channel.connect(new InetSocketAddress(host, port)).get()

      chanelReader = new FramingChannelReader(channel, Frame.MAX_FRAME_SIZE + FramingChannelReader.PAYLOAD_OFFSET)
      closed = false
    }

    val request = Hello(ProtocolVersion.value, BuildInfo.version, reqTime, Map.empty)
    write(request)
    waitFor(Tags.HELLO_RESPONSE, HelloResponse.readFrame[ByteBuffer]) match {
      case Right(response) =>
        if (response.protocolVersion != ProtocolVersion.value) {
          throw new YupanaException(
            error(
              s"Incompatible protocol versions: ${response.protocolVersion} on server and ${ProtocolVersion.value} in this driver"
            )
          )
        }
        if (response.reqTime != reqTime) {
          throw new YupanaException(error("got wrong hello response"))
        }

      case Left(msg) => throw new YupanaException(msg)
    }

    waitFor(Tags.CREDENTIALS_REQUEST, CredentialsRequest.readFrame[ByteBuffer]) match {
      case Right(cr) if cr.method == CredentialsRequest.METHOD_PLAIN =>
        write(Credentials(CredentialsRequest.METHOD_PLAIN, user, password))

      case Right(cr) => throw new YupanaException(error(s"Unsupported auth method ${cr.method}"))
      case Left(msg) => throw new YupanaException(msg)
    }
    waitFor(Tags.AUTHORIZED, Authorized.readFrame[ByteBuffer])
    waitFor(Tags.IDLE, Idle.readFrame[ByteBuffer])
//    scheduleHeartbeatTimer()
  }

  def acquireNext(id: Int): Unit = {
    assert(iterators.contains(id))

    write(Next(id, batchSize))
    var errorMessage: String = null
    var footer: ResultFooter = null

    var count = 0

    do {
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
      waitFor(Tags.IDLE, Idle.readFrame[ByteBuffer])
      if (errorMessage != null) {
        throw new YupanaException(errorMessage)
      }
    }
  }

  @tailrec
  private def waitFor[T](tag: Byte, get: Frame => T): Either[String, T] = {
    val frame = chanelReader.awaitAndReadFrame()
    frame.frameType match {
      case `tag` => Right(get(frame))
      case Tags.HEARTBEAT =>
        val hb = Heartbeat.readFrame(frame)
        heartbeat(hb.time)
        waitFor(tag, get)
      case Tags.ERROR_MESSAGE =>
        val msg = ErrorMessage.readFrame(frame).message
        Left(error(s"Got error response on '${tag.toChar}', '$msg'"))

      case x => Left(error(s"Unexpected response '${x.toChar}' while waiting for '${tag.toChar}'"))
    }
  }

  private def execRequestQuery(id: Int, command: Command[_]): Result = {
    logger.fine(s"Exec request query $command")
    ensureNotClosed()
//    cancelHeartbeatTimer()
    write(command)

    val header = waitFor(Tags.RESULT_HEADER, ResultHeader.readFrame[ByteBuffer])

    header match {
      case Right(h) =>
        val r = new ResultIterator(id, this)
        iterators += id -> r
        extractProtoResult(h, r)

      case Left(e) =>
//        close()
        throw new YupanaException(e)
    }
  }

  private def write(request: Command[_]): Unit = {
    val f = request.toFrame
    val bb = ByteBuffer.allocate(f.payload.length + 4 + 1)
    bb.put(f.frameType)
    bb.putInt(f.payload.length)
    bb.put(f.payload)
    bb.flip()

    while (bb.hasRemaining) {
      val written = channel.write(bb).get()
      if (written == 0) Thread.sleep(1)
    }

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
