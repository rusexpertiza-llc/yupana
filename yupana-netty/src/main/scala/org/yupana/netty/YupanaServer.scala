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
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ Channel, ChannelFuture, ChannelInitializer, ChannelOption }

import scala.concurrent.{ Future, Promise }
import io.netty.handler.timeout.IdleStateHandler

class YupanaServer(host: String, port: Int, nThreads: Int, serverContext: ServerContext) extends StrictLogging {

  private var channel: Channel = _

  def start(): Future[Unit] = {

    if (channel != null) throw new IllegalStateException("Already started")

    val closePromise = Promise[Unit]()
    val parentGroup = new NioEventLoopGroup()
    val childGroup = new NioEventLoopGroup()
    val yupanaGroup = new NioEventLoopGroup(nThreads)
    val bootstrap = new ServerBootstrap()

    bootstrap
      .group(parentGroup, childGroup)
      .channel(classOf[NioServerSocketChannel])
      .childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          ch.pipeline().addLast(new IdleStateHandler(30, 0, 0))
          ch.pipeline().addLast("frame", new FrameCodec())
          ch.pipeline().addLast(yupanaGroup, "handler", new ConnectingHandler(serverContext))
        }
      })
      .option(ChannelOption.SO_BACKLOG, Integer.valueOf(128))
      .childOption(ChannelOption.SO_KEEPALIVE, Boolean.box(true))

    val f = bootstrap.bind(host, port).sync()
    logger.info(s"Starting YupanaServer on $host:$port")
    channel = f.channel()
    f.channel()
      .closeFuture()
      .addListener((_: ChannelFuture) => {
        yupanaGroup.shutdownGracefully()
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
}
