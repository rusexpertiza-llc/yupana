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

package org.yupana.pekko

import com.typesafe.scalalogging.StrictLogging
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Attributes.CancellationStrategy
import org.apache.pekko.stream.{ ActorAttributes, Attributes, Supervision }
import org.apache.pekko.stream.scaladsl.{ Flow, Source, Tcp }
import org.apache.pekko.util.{ ByteString, ByteStringBuilder }
import org.yupana.proto.{ Request, Response }

import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

class TsdbTcp(
    requestHandler: RequestHandler,
    host: String,
    port: Int,
    majorVersion: Int,
    minorVersion: Int,
    version: String
)(implicit val system: ActorSystem)
    extends StrictLogging {

  import system.dispatcher

  private val HEART_BEAT_INTERVAL = 10
  private val FRAME_SIZE = 1024 * 100
  private val REQUEST_SIZE_LIMIT = FRAME_SIZE * 50

  val decider: Supervision.Decider = { e =>
    logger.error("Exception:", e)
    Supervision.Stop
  }

  private val connections = Tcp()
    .bind(host, port, idleTimeout = 60.seconds)
    .withAttributes(ActorAttributes.supervisionStrategy(decider))

  connections runForeach { conn =>
    val sentSize = new AtomicInteger(0)
    val sentChunks = new AtomicInteger(0)

    logger.info(s"Get TCP connection from ${conn.remoteAddress}")

    val heartbeat =
      Source
        .tick(HEART_BEAT_INTERVAL.seconds, HEART_BEAT_INTERVAL.seconds, HEART_BEAT_INTERVAL)
        .scan(0)(_ + _)
        .drop(1)
        .map { time =>
          logger.debug(s"Heartbeat($time), connection: ${conn.remoteAddress}")
          Iterator(Response(Response.Resp.Heartbeat(time.toString)))
        }

    val requestFlow = Flow[ByteString]
      .addAttributes(
        Attributes(CancellationStrategy(CancellationStrategy.AfterDelay(1.second, CancellationStrategy.FailStage)))
      )
      .scan((ByteString.empty, Option.empty[Request])) {
        case ((acc, _), part) =>
          val b = acc.concat(part)
          if (b.length > REQUEST_SIZE_LIMIT) {
            throw new IllegalArgumentException(s"Request is too big")
          }

          unpackRequest(b) match {
            case Some(r) => ByteString.empty -> Some(r)
            case None    => b -> None
          }
      }
      .collect {
        case (_, Some(r)) =>
          logger.debug("Received request" + r)
          r
      }
      .mapAsync(1) {
        case Request(Request.Req.Ping(ping)) =>
          Future.successful(requestHandler.handlePingProto(ping, majorVersion, minorVersion, version))

        case Request(Request.Req.SqlQuery(sqlQuery)) =>
          requestHandler.handleQuery(sqlQuery)

        case Request(Request.Req.BatchSqlQuery(batchSqlQuery)) =>
          requestHandler.handleBatchQuery(batchSqlQuery)

        case Request(Request.Req.Empty) =>
          val error = "Got empty request"
          logger.error(error)
          throw new Exception(error)
      }
      .collect {
        case Right(xs) =>
          xs
        case Left(s) =>
          logger.error(s)
          throw new Exception(s)
      }
      .merge(heartbeat, eagerComplete = true)
      .flatMapConcat { rs =>
        val it = rs.map { resp =>
          packResponse(resp)
        }
        val repacked = new RepackIterator(it, 32768)
        Source.fromIterator(() => repacked)
      }
      .recover {
        case e: Throwable =>
          logger.error("Message was not handled", e)
          val resp = Response(Response.Resp.Error(e.getMessage))
          packResponse(resp)
      }

    val connHandler = requestFlow
      .watchTermination() { (_, done) =>
        done.onComplete {
          case Success(_) =>
            logger.debug(
              s"Connection closed: ${conn.remoteAddress}, bytes sent ${humanReadableByteSize(sentSize.get)}, chunks ${sentChunks.get()} "
            )
          case Failure(ex) =>
            logger.error(s"Error : $ex")
        }
      }

    conn.handleWith(connHandler.withAttributes(ActorAttributes.supervisionStrategy(decider)))
  }

  private def packResponse(response: Response): ByteString = {
    implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN

    val b = new ByteStringBuilder
    val bytes = response.toByteArray
    b.putInt(bytes.length)
    b.putBytes(bytes)
    b.result()
  }

  private def unpackRequest(bs: ByteString): Option[Request] = {
    implicit val byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN

    val it = bs.iterator

    val size = Try {
      it.getInt
    }

    size.toOption.foreach { s =>
      if (s > REQUEST_SIZE_LIMIT) throw new IllegalArgumentException(s"Request size is too big")
    }

    size.map { s =>
      val bytes = it.getBytes(s)
      Request.parseFrom(bytes)
    }.toOption
  }

  private def humanReadableByteSize(fileSize: Long): String = {
    if (fileSize <= 0) return "0 B"
    // kilo, Mega, Giga, Tera, Peta, Exa, Zetta, Yotta
    val units: Array[String] = Array("B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    val digitGroup: Int = (Math.log10(fileSize.toDouble) / Math.log10(1024)).toInt
    if (digitGroup == 0) {
      f"${fileSize / Math.pow(1024, digitGroup)}%3.0f ${units(digitGroup)}"
    } else {
      f"${fileSize / Math.pow(1024, digitGroup)}%3.1f ${units(digitGroup)}"
    }
  }
}
