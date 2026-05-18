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

package org.yupana.netty

import com.typesafe.scalalogging.StrictLogging
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ Channel, ChannelFuture, MultiThreadIoEventLoopGroup }

import scala.concurrent.{ Future, Promise }

abstract class NettyServer(host: String, port: Int, nThreads: Int, name: String)
    extends StrictLogging
    with AutoCloseable {

  private var channel: Channel = _

  def setup(bootstrap: ServerBootstrap): Unit

  def start(): Future[Unit] = {
    if (channel != null) throw new IllegalStateException("Already started")

    val closePromise = Promise[Unit]()
    val parentGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory())
    val childGroup = new MultiThreadIoEventLoopGroup(nThreads, NioIoHandler.newFactory())
    val bootstrap = new ServerBootstrap()

    bootstrap
      .group(parentGroup, childGroup)
      .channel(classOf[NioServerSocketChannel])

    setup(bootstrap)

    val f = bootstrap.bind(host, port).sync()
    logger.info(s"Starting $name on $host:$port")
    channel = f.channel()
    f.channel()
      .closeFuture()
      .addListener((_: ChannelFuture) => {
        childGroup.shutdownGracefully()
        parentGroup.shutdownGracefully()
        closePromise.success(())
      })

    closePromise.future
  }

  def stop(): Unit = {
    if (channel != null) {
      channel.close().sync()
      channel = null
    }
  }

  override def close(): Unit = stop()
}
