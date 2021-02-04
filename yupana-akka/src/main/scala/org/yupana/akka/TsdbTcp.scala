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

package org.yupana.akka

import java.util.concurrent.atomic.AtomicInteger
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Framing, Source, Tcp }
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings, Supervision }
import akka.util.{ ByteString, ByteStringBuilder }
import com.typesafe.scalalogging.StrictLogging
import org.yupana.core.QueryEngineContainer
import org.yupana.proto.{ Request, Response }

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

class TsdbTcp(
    queryEngineContainer: QueryEngineContainer,
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

  val decider: Supervision.Decider = { e =>
    logger.error("Exception:", e)
    Supervision.Stop
  }
  implicit private val mat: ActorMaterializer = ActorMaterializer(
    ActorMaterializerSettings(system).withSupervisionStrategy(decider)
  )

  private val connections = Tcp().bind(host, port, idleTimeout = 60.seconds)

  connections runForeach { conn =>
    val sentSize = new AtomicInteger(0)
    val sentChunks = new AtomicInteger(0)

    logger.info(s"Get TCP connection from ${conn.remoteAddress}")

    val protocol = Framing.simpleFramingProtocol(1024 * 100).reversed

    val heartbeat =
      Source
        .tick(HEART_BEAT_INTERVAL.seconds, HEART_BEAT_INTERVAL.seconds, HEART_BEAT_INTERVAL)
        .scan(0)(_ + _)
        .drop(1)
        .map { time =>
          logger.debug(s"Heartbeat($time), connection: ${conn.remoteAddress}")
          ByteString(Response(Response.Resp.Heartbeat(time.toString)).toByteArray)
        }

    val requestFlow = Flow[ByteString]
      .map { b =>
        val r = Try(Request.parseFrom(b.toArray)) match {
          case Success(message) =>
            message

          case Failure(f) =>
            logger.error(s"Parse error $b: ", f)
            throw f
        }

        logger.debug("Received request" + r)
        r
      }
      .mapAsync(1) {
        case Request(Request.Req.Ping(ping)) =>
          Future.successful(requestHandler.handlePingProto(ping, majorVersion, minorVersion, version))

        case Request(Request.Req.SqlQuery(sqlQuery)) =>
          requestHandler.handleQuery(queryEngineContainer, sqlQuery)

        case Request(Request.Req.BatchSqlQuery(batchSqlQuery)) =>
          requestHandler.handleBatchQuery(queryEngineContainer.timeSeriesQueryEngine, batchSqlQuery)

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
      .flatMapConcat { rs =>
        val it = rs.map { resp =>
          ByteString(resp.toByteArray)
        }
        new AsyncIteratorSource(it, 1000)
      }
      .recover {
        case e: Throwable =>
          logger.error("Message was not handled", e)
          val resp = Response(Response.Resp.Error(e.getMessage))
          ByteString(resp.toByteArray)
      }
      .merge(heartbeat, eagerComplete = true)

    val connHandler = protocol
      .join(requestFlow)
      .groupedWeightedWithin(32767, 100.millis)(_.length)
      .map { bsIt =>
        val b = new ByteStringBuilder()
        b.sizeHint(32767)

        bsIt.foreach(b.append)
        val bs = b.result()
        val ss = sentSize.addAndGet(bs.length)
        val sc = sentChunks.incrementAndGet()
        if (sc % 100 == 0) logger.trace(s"Sent ${humanReadableByteSize(ss)}, $sc chunks")
        bs
      }
      .watchTermination() { (_, done) =>
        done.onComplete {
          case Success(_) =>
            logger.info(
              s"Response sent to client: ${conn.remoteAddress}, bytes ${humanReadableByteSize(sentSize.get)}, ${sentChunks.get()} chunks"
            )
          case Failure(ex) =>
            logger.error(s"Error : $ex")
        }
      }

    conn.handleWith(connHandler)
  }

  def humanReadableByteSize(fileSize: Long): String = {
    if (fileSize <= 0) return "0 B"
    // kilo, Mega, Giga, Tera, Peta, Exa, Zetta, Yotta
    val units: Array[String] = Array("B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    val digitGroup: Int = (Math.log10(fileSize) / Math.log10(1024)).toInt
    f"${fileSize / Math.pow(1024, digitGroup)}%3.3f ${units(digitGroup)}"
  }
}
