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

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.channels.SocketChannel
import java.nio.{ ByteBuffer, ByteOrder }
import java.util.logging.Logger
import org.yupana.api.query.{ Result, SimpleResult }
import org.yupana.api.types.DataType
import org.yupana.api.utils.CollectionUtils
import org.yupana.jdbc.build.BuildInfo
import org.yupana.protocol._
import org.yupana.protocol.ProtocolVersion

import java.util.{ Timer, TimerTask }

class YupanaTcpClient(val host: String, val port: Int) extends AutoCloseable {

  private val logger = Logger.getLogger(classOf[YupanaTcpClient].getName)

  logger.info("New instance of YupanaTcpClient")

  private val CHUNK_SIZE = 1024 * 100
  private val HEARTBEAT_PERIOD = 5000

  private var channel: SocketChannel = _
  private var chanelReader: FramingChannelReader = _

  private var heartbeatTimer: java.util.Timer = _
  private var heartbeatTimerScheduled = false

  private def scheduleHeartbeatTimer(): Unit = {
    heartbeatTimer = new Timer()
    val heartbeatTask = new TimerTask {
      override def run(): Unit = tryToReadHeartbeat()
    }
    heartbeatTimerScheduled = true
    heartbeatTimer.schedule(heartbeatTask, HEARTBEAT_PERIOD, HEARTBEAT_PERIOD)
  }
  private def cancelHeartbeatTimer(): Unit = {
    if (heartbeatTimerScheduled) {
      heartbeatTimerScheduled = false
      heartbeatTimer.cancel()
      heartbeatTimer.purge()
    }
  }

  private def ensureConnected(): Unit = {
    if (channel == null || !channel.isOpen || !channel.isConnected) {
      logger.info(s"Connect to $host:$port")
      channel = SocketChannel.open()
      channel.configureBlocking(false)
      channel.connect(new InetSocketAddress(host, port))
      while (!channel.finishConnect()) {
        Thread.sleep(1)
      }
      chanelReader = new FramingChannelReader(channel, CHUNK_SIZE + 4)
    }
  }

  def query(query: String): Result = {
    execRequestQuery(SimpleQuery(query))
  }

  def prepareQuery(query: String, params: Map[Int, ParameterValue]): Result = {
    execRequestQuery(PrepareQuery(query, params))
  }

  def batchQuery(query: String, params: Seq[Map[Int, ParameterValue]]): Result = {
    val request = creteProtoBatchQuery(query, params)
    execRequestQuery(request)
  }

  def ping(reqTime: Long): Option[Version] = {
    logger.fine("Ping")
    val request = Hello(ProtocolVersion.value, BuildInfo.version, reqTime, Map.empty)
    execPing(request) match {
      case Right(response) =>
        if (response.reqTime != reqTime) {
          throw new Exception("got wrong ping response")
        }
        response.version

      case Left(msg) => throw new IOException(msg)
    }
  }

  private def execPing(request: Hello): Either[String, HelloResponse] = {
    ensureConnected()
    cancelHeartbeatTimer()
    sendRequest(request)
    val frame = chanelReader.awaitAndReadFrame()

    val result = frame.frameType match {
      case Tags.HELLO_RESPONSE =>
        val r = HelloResponse.readFrame(frame)
        if (r.protocolVersion != ProtocolVersion.value) {
          Left(
            error(
              s"Incompatible protocol versions: ${r.protocolVersion} on server and ${ProtocolVersion.value} in this driver"
            )
          )
        } else {
          logger.fine("Received pong response")
          Right(r)
        }

      case Tags.ERROR_MESSAGE =>
        val msg = ErrorMessage.readFrame(frame).message
        Left(error(s"Got error response on ping, '$msg'"))

      case x =>
        Left(error(s"Unexpected response '${x.toChar}' on ping"))

    }

    scheduleHeartbeatTimer()
    result
  }

  private def execRequestQuery(command: Command[_]): Result = {
    logger.fine(s"Exec request query $command")
    cancelHeartbeatTimer()
    ensureConnected()
    sendRequest(command)

    val header = readResultHeader()

    header match {
      case Right(h) =>
        val r = resultIterator()
        extractProtoResult(h, r)

      case Left(e) =>
        close()
        throw new IllegalArgumentException(e)
    }
  }

  private def sendRequest(request: Command[_]): Unit = {
    try {
      write(request)
    } catch {
      case io: IOException =>
        logger.warning(s"Caught $io while trying to write to channel, let's retry")
        Thread.sleep(1000)
        channel = null
        ensureConnected()
        write(request)
    }
  }

  private def write(request: Command[_]): Unit = {
    val chunks = createChunks(request.toFrame[ByteBuffer])
    chunks.foreach { chunk =>
      while (chunk.hasRemaining) {
        val writed = channel.write(chunk)
        if (writed == 0) Thread.sleep(1)
      }
    }
  }

  private def createChunks(data: Frame[ByteBuffer]): Array[ByteBuffer] = {
    data
      .grouped(CHUNK_SIZE)
      .map { ch =>
        val bb = ByteBuffer.allocate(ch.length + 4).order(ByteOrder.BIG_ENDIAN)
        bb.putInt(ch.length)
        bb.put(ch)
        bb.flip()
        bb
      }
      .toArray
  }

  private def readResultHeader(): Either[String, ResultHeader] = {
    val frame = chanelReader.awaitAndReadFrame()

    frame.frameType match {
      case Tags.RESULT_HEADER =>
        logger.fine("Received result header ")
        Right(ResultHeader.readFrame(frame))

      case Tags.RESULT_ROW =>
        Left(error("Data chunk received before header"))

      case Tags.RESULT_FOOTER =>
        Left(error("Unexpected result footer response"))

      case Tags.HEARTBEAT =>
        val hb = Heartbeat.readFrame(frame)
        heartbeat(hb.time)
        readResultHeader()

      case x =>
        Left(error(s"Expect result header frame, but got '${x.toChar}'"))
    }
  }

  private def tryToReadHeartbeat(): Unit = {
    if (channel.isOpen && channel.isConnected) {
      val fr =
        try {
          chanelReader.readFrame()
        } catch {
          case _: IOException => None
        }

      fr.foreach { frame =>
        frame.frameType match {
          case Tags.HEARTBEAT => heartbeat(Heartbeat.readFrame(frame).time)
          case x              => throw new IOException(s"Unexpected response '${x.toChar}'")
        }
      }
    }
  }

  private def error(e: String): String = {
    logger.warning(s"Got error message: $e")
    e
  }

  private def heartbeat(time: Int): Unit = {
    val msg = s"Heartbeat($time)"
    logger.fine(msg)
  }

  private def resultIterator(): Iterator[ResultRow] = {
    new Iterator[ResultRow] {

      var footer: ResultFooter = _
      var current: ResultRow = _
      var errorMessage: String = _

      readNext()

      override def hasNext: Boolean = {
        footer == null
      }

      override def next(): ResultRow = {
        val result = current
        if (footer == null) readNext() else current = null
        result
      }

      private def readNext(): Unit = {
        current = null
        do {
          val frame = chanelReader.awaitAndReadFrame()

          frame.frameType match {
            case Tags.RESULT_ROW =>
              current = ResultRow.readFrame(frame)

            case Tags.RESULT_HEADER =>
              errorMessage = error("Duplicate header received")

            case Tags.HEARTBEAT =>
              heartbeat(Heartbeat.readFrame(frame).time)

            case Tags.ERROR_MESSAGE =>
              errorMessage = error(ErrorMessage.readFrame(frame).message)

            case Tags.RESULT_FOOTER =>
              val ftr = ResultFooter.readFrame(frame)
              logger.fine(s"Got footer $ftr")
              scheduleHeartbeatTimer()
              footer = ftr
          }
        } while (current == null && footer == null && errorMessage == null)

        if (footer != null || errorMessage != null) {
          if (errorMessage != null) {
            close()
            throw new IllegalArgumentException(errorMessage)
          }
        }
      }
    }
  }

  override def close(): Unit = {
    logger.fine("Close connection")
    cancelHeartbeatTimer()
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
