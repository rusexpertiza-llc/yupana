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
import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.protobuf.InvalidProtocolBufferException;
import org.yupana.Proto.*;
import org.yupana.proto.util.ProtocolVersion;

import org.yupana.jdbc.build.BuildInfo;

public class YupanaTcpClient implements AutoCloseable {

  private final String host;
  private final int port;
  private SocketChannel channel;

  private static final int CHUNK_SIZE = 1024 * 100;
  private static final Logger logger = Logger.getLogger(YupanaTcpClient.class.getName());

  public YupanaTcpClient(String host, int port) {
    this.host = host;
    this.port = port;
  }


  private void ensureConnected() throws IOException {
    if (channel == null || !channel.isConnected()) {
      channel = SocketChannel.open();
      channel.configureBlocking(true);
      channel.connect(new InetSocketAddress(host, port));
    }
  }

  public Iterator<Object> query(String query, Map<Integer, ParameterValue> params) throws IOException {
    Request request = createProtoQuery(query, params);
    return execRequestQuery(request);
  }

  public Iterator<Object> batchQuery(String query, Collection<Map<Integer, ParameterValue>> params) throws IOException {
    Request request = creteProtoBatchQuery(query, params);
    return execRequestQuery(request);
  }

  Optional<Version> ping(Long reqTime) throws IOException {
    Request request = createProtoPing(reqTime);
    Pong pong = execPing(request);

    if (pong.getReqTime() != reqTime) {
      throw new ProtocolException("got wrong ping response");
    }
    channel.close();

    return Optional.ofNullable(pong.getVersion());
  }

  private Pong execPing(Request request) throws IOException {
    ensureConnected();
    sendRequest(request);
    Response response = fetchResponse();

    switch (response.getRespCase()) {
      case PONG:
        Pong pong = response.getPong();
        if (pong.getVersion().getProtocol() != ProtocolVersion.value)
          throw new ProtocolException(
                  String.format("Incompatible protocol versions: %d on server and %d in this driver",
                          pong.getVersion().getProtocol(),
                          Protocolversion.value)
          );
        return pong;

      case ERROR:
        throw new IOException(error(String.format("Got error response on ping, '%s'", response.getError())));

      default:
        throw new IOException(error("Unexpected response on ping"));
    }
  }

  private Iterator<Object> execRequestQuery( Request request) throws IOException {
    ensureConnected();
    sendRequest(request);
    FramingChannelIterator it = new FramingChannelIterator(channel, CHUNK_SIZE + 4);

    try {
      Stream<Response> resps = StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false)
              .map(Response::parseFrom);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }

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

  private void sendRequest( Request request) throws IOException {
    try {
      channel.write(createChunks(request.toByteArray()));
    } catch (IOException io) {
      logger.warning(String.format("Caught %s while trying to write to channel, let's retry", io.getMessage()));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      ensureConnected();
      channel.write(createChunks(request.toByteArray()));
    }
  }

  private ByteBuffer[] createChunks(byte [] data) {
    int last = data.length % CHUNK_SIZE;
    int chunks = data.length / CHUNK_SIZE + (last > 0 ? 1 : 0);
    ByteBuffer[] result  = new ByteBuffer[chunks];
    for (int i = 0; i < chunks - (last > 0 ? 1: 0); i++) {
      result[i] = ByteBuffer.allocate(CHUNK_SIZE + 4).order(ByteOrder.BIG_ENDIAN);
      result[i].putInt(CHUNK_SIZE);
      result[i].put(data, i * CHUNK_SIZE, CHUNK_SIZE);
    }

    if (last > 0) {
      result[chunks - 1] = ByteBuffer.allocate(last + 4).order(ByteOrder.BIG_ENDIAN);
      result[chunks - 1].putInt(last);
      result[chunks - 1].put(data, (chunks - 1) * CHUNK_SIZE, last);
    }

    return result;
  }

  private Response fetchResponse() throws IOException {
    ByteBuffer bb = ByteBuffer.allocate(CHUNK_SIZE + 4).order(ByteOrder.BIG_ENDIAN);
    int bytesRead = channel.read(bb);
    if (bytesRead < 0) throw new IOException("Broken pipe");
    else if (bytesRead < 4) throw new IOException("Invalid server response");
    bb.flip();
    int chunkSize = bb.getInt();
    byte[] bytes = new byte[chunkSize];
    bb.get(bytes);
    return Response.parseFrom(bytes);
  }

  private Optional<ResultHeader> handleResultHeader(Response response) throws IOException {
    switch (response.getRespCase()) {
      case RESULTHEADER:
        ResultHeader h = response.getResultHeader();
        logger.info("Received result header " + h);
        return Optional.of(h);

      case RESULT:
        throw new IOException(error("Data chunk received before header"));

      case PONG:
        throw new IOException(error("Unexpected TspPong response"));

      case HEARTBEAT:
        heartbeat(response.getHeartbeat());
        return Optional.empty();

      case ERROR:
        channel.close();
        throw new IOException(error(response.getError()));

      case RESULTSTATISTICS:
        throw new IOException(error("Unexpected ResultStatistics response"));

      case RESP_NOT_SET:
        return Optional.empty();
    }
  }

  private String error(String e) {
    logger.warning(String.format("Got error message: %s", e));
    return e;
  }

  private void heartbeat(String time) {
    logger.info(String.format("Heartbeat(%s)", time));
  }

  private Iterator<ResultChunk> resultIterator(Iterator<Response> responses) throws IOException {
    return new Iterator<ResultChunk>() {

      {
        readNext();
      }

      @Override
      public boolean hasNext() {
        return responses.hasNext() && statistics == null && errorMessage == null;
      }

      @Override
      public ResultChunk next() {
        ResultChunk result = current;
        readNext();
        return result;
      }

      private ResultStatistics statistics = null;
      private ResultChunk current = null;
      private String errorMessage = null;


      private void readNext() throws IOException {
        current = null;
        do {
          Response next = responses.next();
          switch (next.getRespCase()) {
            case RESULT:
              current = next.getResult();
              break;

            case RESULTHEADER:
              errorMessage = error("Duplicate header received");
              break;

            case PONG:
              errorMessage = error("Unexpected Pong response");
              break;

            case HEARTBEAT:
              heartbeat(next.getHeartbeat());
              break;

            case ERROR:
              errorMessage = error(next.getError());
              break;

            case RESULTSTATISTICS:
              logger.fine(String.format("Got statistics %s", next.getResultStatistics().toString()));
              statistics = next.getResultStatistics();
              break;

            case RESP_NOT_SET:
              break;
          }
        } while (current == null && statistics == null && errorMessage == null && responses.hasNext());

        if (statistics != null || errorMessage != null) {
          channel.close();
          if (errorMessage != null) {
            throw new IllegalArgumentException(errorMessage);
          }
        }

        if (!responses.hasNext() && statistics == null) {
          channel.close();
          throw new IllegalArgumentException("Unexpected end of response");
        }
      }
    };
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
                            .setMajor(BuildInfo.majorVersion)
                            .setMinor(BuildInfo.minorVersion)
                            .setVersion(BuildInfo.version)
                    )
            ).build();
  }

  private Iterator<Object> extractProtoResult(ResultHeader header, Iterator<ResultChunk> res) {
    List<String> names = header.getFieldsList().stream().map(ResultField::getName).collect(Collectors.toList());
    List<String> errors = new ArrayList<>();
    List<DataType> dataTypes = new ArrayList<>();

    header.getFieldsList().forEach(x -> {
      if () {
        dataTypes.add();
      } else {
        errors.add(String.format("Unknown type %s", x.getType()));
      }
    });

    if (!errors.isEmpty()) {
      throw new IllegalArgumentException(String.format("Cannot read data: %s", String.join(", ", errors)));
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

    return new SimpleResult(Optional.ofNullable(header.getTableName()).orElse("TABLE"), names, dataTypes, values);
  }

  private Request createProtoQuery(String query, Map<Integer, ParameterValue> params) {
    SqlQuery.Builder sq = SqlQuery.newBuilder().setSql(query);
    params.forEach(sq::setParameters);


    return Request.newBuilder().setSqlQuery(sq).build();
  }

  private Request creteProtoBatchQuery(String query, Collection<Map<Integer, ParameterValue>> params) {

    BatchSqlQuery.Builder sbq = BatchSqlQuery.newBuilder().setSql(query);

    params.forEach(batch -> {
      ParameterValues.Builder values = ParameterValues.newBuilder();
      batch.forEach(values::setParameters);
      sbq.addBatch(values);
    });
    return Request.newBuilder().setBatchSqlQuery(sbq).build();
  }
}
