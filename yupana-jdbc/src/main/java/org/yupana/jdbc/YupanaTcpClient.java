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

package org.yupana.jdbc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.logging.Logger;

import org.yupana.Proto.*;
import org.yupana.proto.util.ProtocolVersion;
import org.yupana.api.query.Result;
import org.yupana.api.types.DataType;
import org.yupana.api.utils.CollectionUtils;
import org.yupana.jdbc.build.BuildInfo;

public class YupanaTcpClient implements AutoCloseable {

  private final String host;
  private final int port;

  public YupanaTcpClient(String host, int port) {
    this.host = host;
    this.port = port;
  }

  private Logger logger = Logger.getLogger(YupanaTcpClient.class.getName());

  private static final int CHUNK_SIZE = 1024 * 100
  private SocketChannel channel;

  private void ensureConnected() throws IOException {
    if (channel == null || !channel.isConnected()) {
      channel = SocketChannel.open();
      channel.configureBlocking(true);
      channel.connect(new InetSocketAddress(host, port));
    }
  }

  public Result query(String query, Map<Integer, ParamValue> params) {
    Request request = createProtoQuery(query, params);
    return execRequestQuery(request);
  }

  public Result batchQuery(String query, Collection<Map<Integer, ParamValue>> params) {
    Request request = creteProtoBatchQuery(query, params);
    return execRequestQuery(request);
  }

  Optional<Version> ping(Long reqTime) {
    Request request = createProtoPing(reqTime)
    execPing(request) match {
      case Right(response) =>
        if (response.reqTime != reqTime) {
          throw new Exception("got wrong ping response")
        }
        channel.close()
        response.version

      case Left(msg) => throw new IOException(msg)
    }
  }

  private  execPing(request: Request): Either[String, Pong] = {
    ensureConnected();
    sendRequest(request)
    val pong = fetchResponse()

    pong.resp match {
      case Response.Resp.Pong(r) =>
        if (r.getVersion.protocol != ProtocolVersion.value) {
          Left(
            error(
              s"Incompatible protocol versions: ${r.getVersion.protocol} on server and ${ProtocolVersion.value} in this driver"
            )
          )
        } else {
          Right(r)
        }

      case Response.Resp.Error(msg) =>
        Left(error(s"Got error response on ping, '$msg'"))

      case _ =>
        Left(error("Unexpected response on ping"))

    }
  }

  private Result execRequestQuery( Request request) {
    ensureConnected();
    sendRequest(request);
    val it = new FramingChannelIterator(channel, CHUNK_SIZE + 4)
      .map(bytes => Response.parseFrom(bytes).resp)

    val header = it.map(resp => handleResultHeader(resp)).find(_.isDefined).flatten

    header match {
      case Some(Right(h)) =>
        val r = resultIterator(it)
        extractProtoResult(h, r)

      case Some(Left(e)) =>
        channel.close()
        throw new IllegalArgumentException(e)

      case None =>
        channel.close();
        throw new IllegalArgumentException(error("Result not received"));
    }
  }

  private void sendRequest( Request request) throws IOException, InterruptedException {
    try {
      channel.write(createChunks(request.toByteArray()));
    } catch (IOException io) {
        logger.warning(String.format("Caught %s while trying to write to channel, let's retry", io.getMessage()));
        Thread.sleep(1000);
        ensureConnected();
        channel.write(createChunks(request.toByteArray()));
    }
  }

  private ByteBuffer[] createChunks(byte [] data) {
    data
      .sliding(CHUNK_SIZE, CHUNK_SIZE)
      .map { ch =>
        val bb = ByteBuffer.allocate(ch.length + 4).order(ByteOrder.BIG_ENDIAN)
        bb.putInt(ch.length)
        bb.put(ch)
        bb.flip()
        bb
      }
      .toArray
  }

  private Response fetchResponse() {
    val bb = ByteBuffer.allocate(CHUNK_SIZE + 4).order(ByteOrder.BIG_ENDIAN)
    val bytesRead = channel.read(bb)
    if (bytesRead < 0) throw new IOException("Broken pipe")
    else if (bytesRead < 4) throw new IOException("Invalid server response")
    bb.flip()
    val chunkSize = bb.getInt()
    val bytes = Array.ofDim[Byte](chunkSize)
    bb.get(bytes)
    Response.parseFrom(bytes)
  }

  private def handleResultHeader(resp: Response.Resp): Option[Either[String, ResultHeader]] = {
    resp match {
      case Response.Resp.ResultHeader(h) =>
        logger.info("Received result header " + h)
        Some(Right(h))

      case Response.Resp.Result(_) =>
        Some(Left(error("Data chunk received before header")))

      case Response.Resp.Pong(_) =>
        Some(Left(error("Unexpected TspPong response")))

      case Response.Resp.Heartbeat(time) =>
        heartbeat(time)
        None

      case Response.Resp.Error(e) =>
        channel.close()
        Some(Left(error(e)))

      case Response.Resp.ResultStatistics(_) =>
        Some(Left(error("Unexpected ResultStatistics response")))

      case Response.Resp.Empty =>
        None
    }
  }

  private String error(String e) {
    logger.warning(String.format("Got error message: %s", e));
    return e;
  }

  private void heartbeat(String time) {
    logger.info(String.format("Heartbeat(%s)", time));
  }

  private Iterator<ResultChunk> resultIterator(Iterator<Response> responses) {
    new Iterator[ResultChunk] {

      var statistics: ResultStatistics = null
      var current: ResultChunk = null
      var errorMessage: String = null

      readNext()

      override def hasNext: Boolean = responses.hasNext && statistics == null && errorMessage == null

      override def next(): ResultChunk = {
        val result = current
        readNext()
        result
      }

      private def readNext(): Unit = {
        current = null
        do {
          responses.next() match {
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
              statistics = stat

            case Response.Resp.Empty =>
          }
        } while (current == null && statistics == null && errorMessage == null && responses.hasNext)

        if (statistics != null || errorMessage != null) {
          channel.close()
          if (errorMessage != null) {
            throw new IllegalArgumentException(errorMessage)
          }
        }

        if (!responses.hasNext && statistics == null) {
          channel.close()
          throw new IllegalArgumentException("Unexpected end of response")
        }
      }

    }
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  private Request createProtoPing(Long reqTime) {
    return Request.newBuilder()
            .setPing(Ping.newBuilder()
                    .setReqTime(reqTime)
                    .setVersion(Version.newBuilder()
                            .setProtocol(ProtocolVersion.value)
                            .setMajor(BuildInfo.majorVersion())
                            .setMinor(BuildInfo.minorVersion())
                            .setVersion(BuildInfo.version())
                    )
            ).build();
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

  private Request createProtoQuery(String query, Map<Integer, ParamValue> params) {
    SqlQuery.Builder sq = SqlQuery.newBuilder().setSql(query);
    params.forEach((idx, pv) -> sq.setParameters(idx, createProtoValue(pv)));


    return Request.newBuilder().setSqlQuery(sq).build();
  }

  private Request creteProtoBatchQuery(String query, List<Map<Integer, ParamValue>> params) {

    BatchSqlQuery.Builder sbq = BatchSqlQuery.newBuilder().setSql(query);

    params.forEach(batch -> {
      ParameterValues.Builder values = ParameterValues.newBuilder();
      batch.forEach((idx, pv) -> values.setParameters(idx, createProtoValue(pv)));
      sbq.addBatch(values);
    });
    return Request.newBuilder().setBatchSqlQuery(sbq).build();
  }

  private ParameterValue createProtoValue(ParamValue value) {
    switch (value.getType()) {
      case NUMERIC:
        return ParameterValue.newBuilder().setValue(Value.newBuilder().setDecimalValue(value.getNumericValue().toString())).build();
      case STRING:
        return ParameterValue.newBuilder().setValue(Value.newBuilder().setTextValue(value.getStringValue())).build();
      case TIMESTAMP:
         return ParameterValue.newBuilder().setValue(Value.newBuilder().setTimeValue(value.getTimestampValue().getTime())).build();
      default:
        throw new IllegalArgumentException(String.format("Unsupported value type %d", value.getType().ordinal()));
    }
  }
}
