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
import org.yupana.jdbc.model.{ NumericValue, StringValue, TimestampValue }
import org.yupana.proto._
import org.yupana.proto.util.ProtocolVersion

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

  def query(query: String, params: Map[Int, model.ParameterValue]): Result = {
    val request = createProtoQuery(query, params)
    execRequestQuery(request)
  }

  def batchQuery(query: String, params: Seq[Map[Int, model.ParameterValue]]): Result = {
    val request = creteProtoBatchQuery(query, params)
    execRequestQuery(request)
  }

  def ping(reqTime: Long): Option[Version] = {
    logger.fine("Ping")
    val request = createProtoPing(reqTime)
    execPing(request) match {
      case Right(response) =>
        if (response.reqTime != reqTime) {
          throw new Exception("got wrong ping response")
        }
        response.version

      case Left(msg) => throw new IOException(msg)
    }
  }

  private def execPing(request: Request): Either[String, Pong] = {
    ensureConnected()
    cancelHeartbeatTimer()
    sendRequest(request)
    val pong = Response.parseFrom(chanelReader.awaitAndReadFrame())

    val result = pong.resp match {
      case Response.Resp.Pong(r) =>
        if (r.getVersion.protocol != ProtocolVersion.value) {
          Left(
            error(
              s"Incompatible protocol versions: ${r.getVersion.protocol} on server and ${ProtocolVersion.value} in this driver"
            )
          )
        } else {
          logger.fine("Received pong response")
          Right(r)
        }

      case Response.Resp.Error(msg) =>
        Left(error(s"Got error response on ping, '$msg'"))

      case _ =>
        Left(error("Unexpected response on ping"))

    }

    scheduleHeartbeatTimer()
    result
  }

  private def execRequestQuery(request: Request): Result = {
    logger.fine(s"Exec request query $request")
    cancelHeartbeatTimer()
    ensureConnected()
    sendRequest(request)

    val header = readResultHeader()

    header match {
      case Right(h) =>
        val r = resultIterator()
        extractProtoResult(h, r)

      case Left(e) =>
        cancelHeartbeatTimer()
        channel.close()
        throw new IllegalArgumentException(e)
    }
  }

  private def sendRequest(request: Request): Unit = {
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

  private def write(request: Request): Unit = {
    val chunks = createChunks(request.toByteArray)
    chunks.foreach { chunk =>
      while (chunk.hasRemaining) {
        val writed = channel.write(chunk)
        if (writed == 0) Thread.sleep(1)
      }
    }
  }

  private def createChunks(data: Array[Byte]): Array[ByteBuffer] = {
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
    val p = chanelReader.awaitAndReadFrame()
    val resp = Response.parseFrom(p).resp

    resp match {
      case Response.Resp.ResultHeader(h) =>
        logger.fine("Received result header " + h)
        Right(h)

      case Response.Resp.Result(_) =>
        Left(error("Data chunk received before header"))

      case Response.Resp.Pong(_) =>
        Left(error("Unexpected TspPong response"))

      case Response.Resp.Heartbeat(time) =>
        heartbeat(time)
        readResultHeader()

      case Response.Resp.Error(e) =>
        channel.close()
        Left(error(e))

      case Response.Resp.ResultStatistics(_) =>
        Left(error("Unexpected ResultStatistics response"))

      case Response.Resp.Empty =>
        readResultHeader()
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
        Response.parseFrom(frame).resp match {
          case Response.Resp.Heartbeat(time) => heartbeat(time)
          case Response.Resp.Empty           =>
          case _                             => throw new IOException("Unexpected response")
        }
      }
    }
  }

  private def error(e: String): String = {
    logger.warning(s"Got error message: $e")
    e
  }

  private def heartbeat(time: String): Unit = {
    val msg = s"Heartbeat($time)"
    logger.fine(msg)
  }

  private def resultIterator(): Iterator[ResultChunk] = {
    new Iterator[ResultChunk] {

      var statistics: ResultStatistics = _
      var current: ResultChunk = _
      var errorMessage: String = _

      readNext()

      override def hasNext: Boolean = {
        statistics == null
      }

      override def next(): ResultChunk = {
        val result = current
        if (statistics == null) readNext() else current = null
        result
      }

      private def readNext(): Unit = {
        current = null
        do {
          val resp = Response.parseFrom(chanelReader.awaitAndReadFrame()).resp

          resp match {
            case Response.Resp.Result(result) =>
              current = result

            case Response.Resp.ResultHeader(_) =>
              errorMessage = error("Duplicate header received")

            case Response.Resp.Pong(_) =>
              errorMessage = error("Unexpected TspPong response")

            case Response.Resp.Heartbeat(time) =>
              heartbeat(time)

            case Response.Resp.Error(e) =>
              errorMessage = error(e)

            case Response.Resp.ResultStatistics(stat) =>
              logger.fine(s"Got statistics $stat")
              scheduleHeartbeatTimer()
              statistics = stat

            case Response.Resp.Empty =>
          }
        } while (current == null && statistics == null && errorMessage == null)

        if (statistics != null || errorMessage != null) {
          if (errorMessage != null) {
            channel.close()
            throw new IllegalArgumentException(errorMessage)
          }
        }
      }
    }
  }

  override def close(): Unit = {
    logger.fine("Close connection")
  }

  private def createProtoPing(reqTime: Long): Request = {
    Request(
      Request.Req.Ping(
        Ping(
          reqTime,
          Some(Version(ProtocolVersion.value, BuildInfo.majorVersion, BuildInfo.minorVersion, BuildInfo.version))
        )
      )
    )
  }

  private def extractProtoResult(header: ResultHeader, res: Iterator[ResultChunk]): Result = {
    val names = header.fields.map(_.name)
    val dataTypes = CollectionUtils.collectErrors(header.fields.map { resultField =>
      DataType.bySqlName(resultField.`type`).toRight(s"Unknown type ${resultField.`type`}")
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
              rt.storable.read(bytes.toByteArray)
            }
        }
        .toArray
    }

    SimpleResult(header.tableName.getOrElse("TABLE"), names, dataTypes, values)
  }

  private def createProtoQuery(query: String, params: Map[Int, model.ParameterValue]): Request = {
    Request(
      Request.Req.SqlQuery(
        SqlQuery(
          query,
          params.map {
            case (i, v) => ParameterValue(i, createProtoValue(v))
          }.toSeq
        )
      )
    )
  }

  private def creteProtoBatchQuery(query: String, params: Seq[Map[Int, model.ParameterValue]]): Request = {
    Request(
      Request.Req.BatchSqlQuery(
        BatchSqlQuery(
          query,
          params.map(vs =>
            ParameterValues(vs.map {
              case (i, v) => ParameterValue(i, createProtoValue(v))
            }.toSeq)
          )
        )
      )
    )
  }

  private def createProtoValue(value: model.ParameterValue): Value = {
    value match {
      case NumericValue(n)   => Value(Value.Value.DecimalValue(n.toString()))
      case StringValue(s)    => Value(Value.Value.TextValue(s))
      case TimestampValue(m) => Value(Value.Value.TimeValue(m))
    }
  }
}
